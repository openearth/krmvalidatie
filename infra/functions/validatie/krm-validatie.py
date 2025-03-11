import uuid
import boto3
from botocore.exceptions import NoCredentialsError
import zipfile
import io
from io import StringIO
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely import wkt
from shapely.geometry import Point
import csv
import os
from urllib.parse import unquote_plus
import urllib3
from datetime import datetime
import json

s3 = boto3.client('s3')
databundel_code = ""
package_name = ""
bundel_akkoord = True
akkoord_file = False

is_local = True

def extract_csv_from_zip_in_s3(bucket_name, zip_file_key):
    global akkoord_file
    try:
        # Download the zip file from S3
        decoded_key = unquote_plus(zip_file_key)

        databundel_code = decoded_key.split('/')[-1]
        
        zip_obj = s3.get_object(Bucket=bucket_name, Key=decoded_key)
        zip_data = zip_obj['Body'].read()

                # Open the zip file in memory
        with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
            file_list = z.namelist()  # Get a list of all the files in the zip
            print("Files in ZIP:", file_list)
            csv_content = None  # Initialize csv_content
            
            # Check if akkoord.txt exists
            if "akkoord.txt" in file_list:
                print("Akkoord text found")
                akkoord_file = True

            # Iterate through the files to find a CSV file and check for akkoord.txt
            for file_name in file_list:            
                if file_name.endswith('.csv'):
                    # Found a CSV file, open and read it
                    with z.open(file_name) as csvfile:
                        # Read the CSV file in memory
                        with io.TextIOWrapper(csvfile, encoding='cp1252') as textfile:
                            csv_content = pd.read_csv(textfile, delimiter=';')
                    break  # Stop after reading the first CSV file

            if csv_content is not None and not csv_content.empty:
                return csv_content
            else:
                print("No CSV content found.")
                return None

    except Exception as e:
        raise RuntimeError(f"Error extracting or reading CSV from zip in S3: {str(e)}")


def validate_records(gdf, package_name):

    global bundel_akkoord

    validatielijst = get_data_from_github('https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/main/data/validatielijst.csv')
    
    print("Validatielijst loaded")
    print(f"Validatielijst columns: {validatielijst.columns}")
    print(f"Validatielijst head: {validatielijst.head()}")
    
    group = get_data_from_github('https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/main/data/groep.csv')

    print("Group loaded")
    print(f"Group columns: {group.columns}")
    print(f"Group head: {group.head()}")

    #column_definition = pd.read_csv('kolomdefinitie.csv', delimiter=';')
    column_definition = get_data_from_github('https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/main/data/kolomdefinitie.csv')

    print("Determining rules")

    rules = determine_rule(gdf, validatielijst, package_name, group)
    print(rules)
    print("Rules determined")
    print(f"Rules columns: {rules.columns}")
    print(f"Rules head: {rules.head()}")

    print("Start Geocontrol")
    # geocontrole (validatie.locatie)
    geo_control_result = geocontrol(gdf, package_name)

    print(geo_control_result)

    print("Start column check (verplichtekolom)")
    # verplichte kolommen check (validatie.verplichtekolom)
    column_check_result = column_check(gdf, validatielijst, package_name, column_definition)
    print(column_check_result)
    
    print("Start column value check (kolomwaarde)")
    # kolomwaarde-controle (validatie.kolomwaarde)
    column_value_result = column_value_check(gdf, validatielijst, package_name)
    print(column_value_result)

    print("Start count check (aantal)")
    # aantal controle (validatie.aantal)
    count_check_result = count_check(gdf, validatielijst, package_name, rules, group)
    print(count_check_result)

    print("Start parameter check (parameter)")
    # parameter controle (validatie.parameter)
    parameter_check_result = parameter_check(gdf, validatielijst, package_name, rules)
    print(parameter_check_result)
    
    print("Start parameter aggregate check (parameterverzameling)")
    # parameter verzameling (validatie.parameterverzameling)
    parameter_aggregate_result = parameter_aggregate(gdf, validatielijst, package_name, rules, group)
    print(parameter_aggregate_result)

    print("Start value check (vastewaarde)")
    # vaste waarden controle (validatie.vastewaarde)
    value_check_result = value_check(gdf, package_name)
    print(value_check_result)

    print("Start regel check (regelcontrole)")
    # vangnet (validatie.regelcontrole)
    regel_check_result = regel_check(rules)
    print(regel_check_result)

    print("Start other checks (vangnet)")
    # overige (validatie.overige)
    other_checks_result = other_checks(gdf, validatielijst, package_name)
    print(other_checks_result)

    print("Start date range check (datumbereik)")
    # controle datumbereik (validatie.datumbereik)
    result_date_range_check = date_range_check(gdf, validatielijst, package_name)
    print(result_date_range_check)

    # List of result dataframes to check
    results = [
        geo_control_result,
        column_check_result,
        column_value_result,
        count_check_result,
        parameter_check_result,
        parameter_aggregate_result,
        value_check_result,
        regel_check_result,
        other_checks_result,
        result_date_range_check
    ]

    # Check if any dataframe is not empty and set flag to False if so
    for result in results:
        if not result.empty:
            print("Bundel niet akkoord")
            bundel_akkoord = False
            break  # Stop checking further if any dataframe is not empty

    print("Creating report")
    print(bundel_akkoord)
    # Create the CSV report
    csv_report = []

    # Define headers for the CSV file
    csv_report.append(["Section", "Databundelcode", "Record ID", "Uitvalreden", "Informatie"])

    # Add Geo controle section
    for row in geo_control_result.itertuples(index=False):
        csv_report.append(["Geo controle", row.databundelcode, row.record_id, row.uitvalreden, row.informatie])

    # Add Verplichte kolommen controle section
    for row in column_check_result.itertuples(index=False):
        csv_report.append(["Verplichte kolommen controle", row.databundelcode, row.record_id, row.uitvalreden, row.informatie])

    # Add Kolomwaarden controle section
    for row in column_value_result.itertuples(index=False):
        csv_report.append(["Kolomwaarde controle", row.databundelcode, row.record_id, row.uitvalreden, row.informatie])

    # Add Aantal controle section
    for row in count_check_result.itertuples(index=False):
        csv_report.append(["Aantal controle", row.databundelcode, row.record_id, row.uitvalreden, row.informatie])

    # Add Parameter controle section
    for row in parameter_check_result.itertuples(index=False):
        csv_report.append(["Parameter controle", row.databundelcode, row.record_id_x, row.uitvalreden, row.informatie])

    # Add Parameter verzameling controle section
    for row in parameter_aggregate_result.itertuples(index=False):
        csv_report.append(["Parameter verzameling controle", row.databundelcode, row.record_id, row.uitvalreden, row.informatie])

    # Add Vaste waarden controle section
    for row in value_check_result.itertuples(index=False):
        csv_report.append(["Vaste waarden controle", row.databundelcode, row.record_id, row.uitvalreden, row.informatie])

    # Add Regel controle section
    for row in regel_check_result.itertuples(index=False):
        csv_report.append(["Regel controle", row.databundelcode, row.record_id, row.uitvalreden, row.informatie])

    # Add Overige controle section
    for row in other_checks_result.itertuples(index=False):
        csv_report.append(["Overige controle", row.databundelcode, row.record_id, row.uitvalreden, row.informatie])

    # Add Datumbereik controle section
    for row in result_date_range_check.itertuples(index=False):
        csv_report.append(["Datumbereik controle", row.databundelcode, row.record_id, row.uitvalreden, row.informatie])

    # Write the CSV report to a file
    report_name = package_name.replace('+', ' ')
    if is_local:
        filename = f'{report_name}.csv'
    else:
        filename = f'/tmp/{report_name}.csv'

    with open(filename, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerows(csv_report)

    upload_file_to_s3(filename, 'krm-validatie-data-prod', f'rapportages/{report_name}.csv')

    print(f"Databundel akkoord: {bundel_akkoord}")

    df = set_criteria(gdf, validatielijst, package_name)

    return df

def set_criteria(df, validatielijst, package_name):

    package_name = package_name.replace('+', ' ')
    validatie_regels = validatielijst[validatielijst['databundelcode'].apply(lambda x: package_name.startswith(x))]

    criteria = validatie_regels['criteria'].values[0]

    df['krmcriterium'] = f"ANSNL-{criteria}"

    return df

def geocontrol(df, package_name):

    validatie_resultaat = pd.DataFrame()

    package_name = package_name.replace('+', ' ')
    
    # Read shapefile
    get_shape_data_from_github(f'https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/main/data/KRM_locatiedetails/KRM2_P.shp', 'KRM2_P.shp')
    get_shape_data_from_github(f'https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/main/data/KRM_locatiedetails/KRM2_P.shx', 'KRM2_P.shx')
    get_shape_data_from_github(f'https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/main/data/KRM_locatiedetails/KRM2_P.prj', 'KRM2_P.prj')
    get_shape_data_from_github(f'https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/main/data/KRM_locatiedetails/KRM2_P.dbf', 'KRM2_P.dbf')
    get_shape_data_from_github(f'https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/main/data/KRM_locatiedetails/KRM2_P.cpg', 'KRM2_P.cpg')
    get_shape_data_from_github(f'https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/main/data/KRM_locatiedetails/KRM2_V.shp', 'KRM2_V.shp')
    get_shape_data_from_github(f'https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/main/data/KRM_locatiedetails/KRM2_V.shx', 'KRM2_V.shx')
    get_shape_data_from_github(f'https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/main/data/KRM_locatiedetails/KRM2_V.prj', 'KRM2_V.prj')
    get_shape_data_from_github(f'https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/main/data/KRM_locatiedetails/KRM2_V.dbf', 'KRM2_V.dbf')
    get_shape_data_from_github(f'https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/main/data/KRM_locatiedetails/KRM2_V.cpg', 'KRM2_V.cpg')

    # Convert the coordinates from the array into shapely Points
    points_from_array = [Point(xy) for xy in zip(df['geometriepunt.x'], df['geometriepunt.y'])]
        
    if is_local:
        gdf_p = gpd.read_file('KRM2_P.shp')
        gdf_v = gpd.read_file('KRM2_V.shp')
    else:
        gdf_p = gpd.read_file('/tmp/KRM2_P.shp')
        gdf_v = gpd.read_file('/tmp/KRM2_V.shp')

    # Concatenate the GeoDataFrames
    combined_gdf = pd.concat([gdf_p, gdf_v], ignore_index=True)

    # Ensure it remains a GeoDataFrame
    combined_gdf = gpd.GeoDataFrame(combined_gdf, geometry='geometry')

    mpindents = []

    # Access the geometry and attributes of the data points
    for index, row in combined_gdf.iterrows():
        geometry = row['geometry']  # The geometry of the feature
        attributes = row.drop('geometry')  # All other attributes excluding geometry
        mpindents.append(attributes.MPNIDENT)

    df['locatie.code'] = df['meetobject.lokaalid'].str.replace('NL80_', '', regex=False)

    # Create a GeoDataFrame for points from the array
    gdf_array = gpd.GeoDataFrame(df, geometry=points_from_array, crs="EPSG:4258")

    gdf_array['Meetobject.lokaalID_cleaned'] = gdf_array['meetobject.lokaalid'].str.replace('NL80_', '', regex=False)

    # Check if 'Meetobject.lokaalID_cleaned' is in the array
    mask = gdf_array['Meetobject.lokaalID_cleaned'].isin(mpindents)

    false_condition_gdf = gdf_array[~mask]

    new_rows = []
    if len(false_condition_gdf) != 0:       

        # Iterate through each record in false_condition_gdf
        for _, row in false_condition_gdf.iterrows():
            # Create a new row as a dictionary
            new_row = {
                'databundelcode': package_name,
                'record_id': row['meetwaarde.lokaalid'].replace('NL80_', ''),  # Remove 'NL80_'
                'uitvalreden': 'onbekende locatie',
                'informatie': f"onbekende locatie: {row['locatie.code']}"  # Access row data correctly
            }
            
            # Append the new row to the validatie_resultaat DataFrame
            new_rows.append(new_row)

    gdf_projected  = combined_gdf.to_crs("EPSG:32631")
    gdf_projected_array = gdf_array.to_crs("EPSG:32631")

    # Merge the dataframes on `mpnident`
    merged_gdf = gdf_projected_array.merge(
        gdf_projected[['MPNIDENT', 'geometry']], 
        left_on='Meetobject.lokaalID_cleaned', 
        right_on='MPNIDENT', 
        suffixes=('_array', '_shapefile')
    )

    new_rows = []

    for _, row in merged_gdf.iterrows():
        point_array = row['geometry_array']
        point_shapefile = row['geometry_shapefile']

        #Calculate the distance
        distance = point_array.distance(point_shapefile)
        
        if distance > 100:  # Not within 100m
            new_row = {
                'databundelcode': package_name,
                'record_id': row['meetwaarde.lokaalid'].replace('NL80_', ''),  # Remove 'NL80_'
                'uitvalreden': 'locatie verder dan 100 meter',
                'informatie': f"afstand van locatie: {row['locatie.code']}: {int(distance)}m"
            }
            
            # Append the new row to the validatie_resultaat DataFrame
            new_rows.append(new_row)
    
    new_rows_df = pd.DataFrame(new_rows)
    validatie_resultaat = pd.concat([validatie_resultaat, new_rows_df], ignore_index=True)

    return validatie_resultaat

def column_check(df, validatielijst, package_name, column_definition):

    package_name = package_name.replace('+', ' ')

    validatie_resultaat = pd.DataFrame()

    package_name = package_name.replace('+', ' ')

    filtered_validatielijst = validatielijst[validatielijst['databundelcode'].apply(lambda x: package_name.startswith(x))]

    if len(filtered_validatielijst) == 0:
        print('Databundel niet gevonden in validatielijst')
        return False

    mandatory_columns = column_definition[column_definition['ihm_verplicht'] == 'V']['kolomnaam'].str.lower()

    new_rows = []
    # Iterate through each record in df
    for index, row in df.iterrows():
        for column in mandatory_columns:
            if pd.isna(row.get(column)):
                # Create a new row as a dictionary
                new_row = {
                    'databundelcode': package_name,
                    'record_id': row.get('meetwaarde.lokaalid', '').replace('NL80_', ''),  # Remove 'NL80_'
                    'uitvalreden': 'verplichte kolom is leeg',
                    'informatie': f"geen waarde in bestand voor: {column}"  # Access row data correctly
                }
                # Append the new row to the validatie_resultaat DataFrame
                new_rows.append(new_row)
    
    new_rows_df = pd.DataFrame(new_rows)
    validatie_resultaat = pd.concat([validatie_resultaat, new_rows_df], ignore_index=True)

    return validatie_resultaat

def column_value_check(df, validatielijst, package_name):
    '''
        uitgangspunten:
        - de combinatie van kolomwaarden in een datarecord moet gelijk zijn aan precies één van de validatieregels behorende bij die databundel
        - qua kolomwaarden sluiten de validatieregels elkaar uit (per databundel)
        - als er tenminste één kolomwaarde nooit geldig is, dan leidt dat tot uitval (uitvalreden 1)
        - als alle kolomwaarden op zich geldig zijn, maar er is toch geen match met één validatieregel, dan ligt het wrs aan de combinatie van kolomwaarden (uitvalreden 2)
        - een lege waarde betekent dat de waarde in de validatieregel ook leeg moet zijn (en vice versa)
        - de kolom Identificatie maakt een datarecord uniek
        - de databundelcode is als kolom beschikbaar gemaakt in de datatabel
        - inhoudelijke terugkoppeling over de uitval komt in de kolom informatie. 
        bij uitvalreden 1 wordt geen informatie gegeven over een mogelijke uitvalreden 2 (dat zou teveel onbruikbare infomratie geven).
    '''
    results = []
    
    package_name = package_name.replace('+', ' ')

    # Get validation rules for the current databundelcode
    rules = validatielijst[validatielijst['databundelcode'].apply(lambda x: package_name.startswith(x))]

    df['locatie.code'] = df['meetobject.lokaalid'].str.replace('NL80_', '', regex=False)

    for _, row in df.iterrows():
        record_results = {
            'databundelcode': package_name,
            'record_id': row['meetwaarde.lokaalid'].replace('NL80_', ''),
            'uitvalreden': None,
            'informatie': None
        }

        # Initialize mismatch counters
        mismatch_counts = {
            'grootheid': 0,
            'typering': 0,
            'eenheid': 0,
            'hoedanigheid': 0,
            'waardebewerkingsmethode': 0,
            'compartimentcode': 0,
            'locatiecode': 0,
            'veldapparaatomschrijving': 0,
            'organismenaam': 0
        }

        # Check against validation rules
        for _, rule in rules.iterrows():
            if not (pd.isna(row['grootheid.code']) and pd.isna(rule['grootheid_code'])) and str(row['grootheid.code']) not in str(rule['grootheid_code']):
                mismatch_counts['grootheid'] += 1
            if not (pd.isna(row['typering.code']) and pd.isna(rule['typering_code'])) and str(row['typering.code']) not in str(rule['typering_code']):
                mismatch_counts['typering'] += 1
            if not (pd.isna(row['eenheid.code']) and pd.isna(rule['eenheid_code'])) and str(row['eenheid.code']) not in str(rule['eenheid_code']):
                mismatch_counts['eenheid'] += 1
            if not (pd.isna(row['hoedanigheid.code']) and pd.isna(rule['hoedanigheid_code'])) and str(row['hoedanigheid.code']) not in str(rule['hoedanigheid_code']):
                mismatch_counts['hoedanigheid'] += 1
            if not (pd.isna(row['waardebewerkingsmethode.code']) and pd.isna(rule['waardebewerkingsmethode_code'])) and str(row['waardebewerkingsmethode.code']) not in str(rule['waardebewerkingsmethode_code']):
                mismatch_counts['waardebewerkingsmethode'] += 1
            if not (pd.isna(row['monstercompartiment.code']) and pd.isna(rule['monstercompartiment_code'])) and str(row['monstercompartiment.code']) not in str(rule['monstercompartiment_code']):
                mismatch_counts['compartimentcode'] += 1
            if not (pd.isna(row['locatie.code']) and pd.isna(rule['locatiecode'])) and rule['locatiecode'].find(row['locatie.code']) == -1:
                mismatch_counts['locatiecode'] += 1
            if not (pd.isna(row['bemonsteringsapparaat.omschrijving']) and pd.isna(rule['bemonsteringsapparaat_omschrijving'])) and str(row['bemonsteringsapparaat.omschrijving']) not in str(rule['bemonsteringsapparaat_omschrijving']):
                mismatch_counts['veldapparaatomschrijving'] += 1
            if not (pd.isna(row['organisme.naam']) and pd.isna(rule['organisme_naam'])) and str(row['organisme.naam']) not in str(rule['organisme_naam']):
                mismatch_counts['organismenaam'] += 1
            
        # Determine uitvalreden and informatie
        record_results['uitvalreden'] = ''
        record_results['informatie'] = ''

        total_rules = len(rules)
        if mismatch_counts['grootheid'] == total_rules:
            record_results['uitvalreden'] += 'ongeldige code'
            record_results['informatie'] += f"Grootheid.code '{row['grootheid.code']}' niet in: {{{','.join(rules['grootheid_code'])}}}."
        elif mismatch_counts['typering'] == total_rules:
            record_results['uitvalreden'] += 'ongeldige code'
            record_results['informatie'] += f"Typering.code '{row['typering.code']}' niet in: {{{','.join(rules['typering_code'])}}}."        
        elif mismatch_counts['eenheid'] == total_rules:
            record_results['uitvalreden'] += 'ongeldige code'
            record_results['informatie'] += f"Eenheid.code '{row['eenheid.code']}' niet in: {{{','.join(rules['eenheid_code'])}}}."
        elif mismatch_counts['hoedanigheid'] == total_rules:
            record_results['uitvalreden'] += 'ongeldige code'
            record_results['informatie'] += f"Hoedanigheid.code '{row['hoedanigheid.code']}' niet in: {{{','.join(rules['hoedanigheid_code'])}}}."
        elif mismatch_counts['waardebewerkingsmethode'] == total_rules:
            record_results['uitvalreden'] += 'ongeldige code'
            record_results['informatie'] += f"Waardebewerkingsmethode '{row['waardebewerkingsmethode.code']}' niet in: {{{','.join(rules['waardebewerkingsmethode_code'])}}}."
        elif mismatch_counts['compartimentcode'] == total_rules:
            record_results['uitvalreden'] += 'ongeldige code'
            record_results['informatie'] += f"Compartimentcode '{row['monstercompartiment.code']}' niet in: {{{','.join(rules['monstercompartiment_code'])}}}."
        elif mismatch_counts['locatiecode'] == total_rules:
            record_results['uitvalreden'] += 'ongeldige code'
            record_results['informatie'] += f"Locatiecode '{row['locatie.code']}' niet in: {{{','.join(rules['locatiecode'])}}}."
        elif mismatch_counts['veldapparaatomschrijving'] == total_rules:
            record_results['uitvalreden'] += 'ongeldige code'
            record_results['informatie'] += f"Veldapparaatomschrijving '{row['bemonsteringsapparaat.omschrijving']}' niet in: {{{','.join(rules['bemonsteringsapparaat_omschrijving'])}}}."
        elif mismatch_counts['organismenaam'] == total_rules:
            record_results['uitvalreden'] += 'ongeldige code'
            record_results['informatie'] += f"Organismenaam '{row['organisme.naam']}' niet in: {{{','.join(rules['organisme_naam'])}}}."
        # else:
        #     record_results['uitvalreden'] = 'ongeldige combinatie'
        #     record_results['informatie'] = 'deze monster-tijdwaarde past niet in de validatielijst van deze bundel'

        if record_results['uitvalreden'] != '':
            results.append(record_results)


    validatie_resultaat = pd.DataFrame()

    new_rows_df = pd.DataFrame(results)

    validatie_resultaat = pd.concat([validatie_resultaat, new_rows_df], ignore_index=True)

    return validatie_resultaat

def count_check(df, validatielijst, package_name, rules, group):
    ''' 
        uitgangspunten:
        - de kolom recordnr maakt een datarecord uniek
        - er is voor een datarecord precies één match met de tabel validatieregels
        - de validatieregels zijn te matchen op deze kolommen: eenheid,grootheid,typering,hoedanigheid,waardebewerkingsmethode,locatiecode
        - wanneer er een match is kan er gecontroleerd worden op aantal (i.c.m. limiet en datumbereik)
        - als er een monster is: aantal monsters tellen (dat.monster_lokaalid)
        - als er geen monster is: aantal tijdwaarden tellen
	'''

    validatie_resultaat = pd.DataFrame()
    package_name = package_name.replace('+', ' ')

    tellingen_df = tellingen(df, validatielijst, package_name, rules, group)
    print(tellingen_df)

    report_tellingen(tellingen_df, package_name)

    new_rows = []
    if len(tellingen_df) != 0:       

        # Iterate through each record in false_condition_gdf
        for _, row in tellingen_df.iterrows():
            if row['uitvalreden'] != "":

                # Create a new row as a dictionary
                new_row = {
                    'databundelcode': package_name,
                    'record_id': row['record_id'].replace('NL80_', ''),  # Remove 'NL80_'
                    'uitvalreden': row['uitvalreden'],
                    'informatie': f"aantal datarecords: {row['aantaldat']} . aantal verwacht: {row['limiet']} {row['aantalval']}"  # Access row data correctly
                }

                # Append the new row to the validatie_resultaat DataFrame
                new_rows.append(new_row)

    new_rows_df = pd.DataFrame(new_rows)
    validatie_resultaat = pd.concat([validatie_resultaat, new_rows_df], ignore_index=True)
    
    return validatie_resultaat

def parameter_check(df, validatielijst, package_name, rules):

    validatie_resultaat = pd.DataFrame()
    package_name = package_name.replace('+', ' ')
    validatie_regels = validatielijst[validatielijst['databundelcode'].apply(lambda x: package_name.startswith(x))]

    #rules.to_csv('r.csv')

    validatie_regels["locatiecode"] = validatie_regels["locatiecode"].str.split(";")
    validatie_regels = validatie_regels.explode("locatiecode")

    df['cleaned_lokaalid'] = df['monster.lokaalid'].str.replace('NL80_', '')
    df['cleaned_meetwaarde_lokaalid'] = df['meetwaarde.lokaalid'].str.replace('NL80_', '')
    df['locatiecode'] = df['meetobject.lokaalid'].str.replace('NL80_', '')

    df['parameter'] = df.apply(
        lambda row: row['biotaxon.naam'] if pd.notnull(row['biotaxon.naam']) and pd.isnull(row['parameter.code']) 
        else (row['parameter.code'] if pd.notnull(row['parameter.code']) and pd.isnull(row['biotaxon.naam']) 
            else f"{row['parameter.code']} / {row['biotaxon.naam']}"),
        axis=1
    )

    validatie_regels.index = validatie_regels.index + 2

    # Filter regelbepalen_df for rows with uitvalreden in (1, 2, 3)
    filtered_val = rules[rules['uitvalreden'].isin([1, 2, 3])]

    # Join dat with filtered_val on recordid
    merged_with_val = pd.merge(df, filtered_val, right_on='record_id', left_on='cleaned_meetwaarde_lokaalid')

    # Reset the index of validatieregels_df and use the index as validatieregel
    validatie_regels = validatie_regels.reset_index().rename(columns={'index': 'validatieregel'})

    # Join the result with validatieregels_df on validatieregel
    merged_with_regels = pd.merge(
        merged_with_val, 
        validatie_regels[['validatieregel', 'groep']],
        on='validatieregel',
        how='inner'
    )

    #merged_with_regels.to_csv('merged_with_regels.csv')

    # Add 'val_groep' by lowering the 'groep' column
    merged_with_regels['val_groep'] = merged_with_regels['groep'].str.lower()

    # Filter de rijen waar 'parameter' gelijk is aan "nan / nan" en 'val_groep' leeg is
    filtered_df = merged_with_regels[
        ~((merged_with_regels['parameter'] == "nan / nan") & (merged_with_regels['val_groep'].isna()))
    ]

    # Build the "informatie" column
    filtered_df['informatie'] = (
        'parameter "' + filtered_df['parameter'].fillna('') +
        '" i.c.m. groep (' + filtered_df['val_groep'].fillna('') +
        ') uit de validatieregel komt niet voor in de groep-lijst'
    )

    # Select and rename columns for the final result
    result = filtered_df.assign(
        uitvalreden='parameter is ongeldig'
    )[[
        'databundelcode', 'record_id_x', 'uitvalreden', 'informatie'
    ]]
   
    new_rows_df = pd.DataFrame(result)
    validatie_resultaat = pd.concat([validatie_resultaat, new_rows_df], ignore_index=True)

    return validatie_resultaat

def parameter_aggregate(df, validatielijst, package_name, rules, group):
    
    package_name = package_name.replace('+', ' ')
    validatie_regels = validatielijst[validatielijst['databundelcode'].apply(lambda x: package_name.startswith(x))]

    # df['cleaned_lokaalid'] = df['meetwaarde.lokaalid'].str.replace('NL80_', '')

    df['parameter'] = df.apply(
        lambda row: row['biotaxon.naam'] if pd.notnull(row['biotaxon.naam']) and pd.isnull(row['parameter.code']) 
        else (row['parameter.code'] if pd.notnull(row['parameter.code']) and pd.isnull(row['biotaxon.naam']) 
            else f"{row['parameter.code']} / {row['biotaxon.naam']}"),
        axis=1
    )

    validatie_regels.index = validatie_regels.index + 2

    #rules.set_index('validatieregel', inplace=True)

    filtered_rules = rules.dropna(subset=['validatieregel'])

    verzamelingen = (
        filtered_rules
        .merge(validatie_regels, left_on='validatieregel', right_index=True)
        .merge(group, how='left', left_on='groep', right_on='groep')
        .assign(
            group=lambda x: x['groep'].str.lower(),
            parameter=lambda x: x['parameter'].str.lower()
        )
        .query("betreftverzameling == 1 and uitvalreden == 0")
        .groupby(['groep', 'parameter'])
        .agg({
            'record_id': 'min',
            'databundelcode_x': 'min'
        })
        .reset_index()
    )

    # Finding missing parameters
    missing_parameters = verzamelingen[~verzamelingen['parameter'].isin(df['parameter'].str.lower())]

    # Prepare result DataFrame
    validatie_resultaat = pd.DataFrame()
    if len(missing_parameters) != 0:
        result = pd.DataFrame({
            'databundelcode': missing_parameters['databundelcode_x'],
            'record_id': missing_parameters['record_id'],
            'uitvalreden': 'ontbrekende parameter',
            'informatie': missing_parameters.apply(lambda row: f'parameter "{row.parameter}" uit groep "{row.group}" niet gevonden', axis=1)
        })

        validatie_resultaat = pd.concat([validatie_resultaat, result], ignore_index=True)

    return validatie_resultaat

def value_check(df, package_name):
    
    package_name = package_name.replace('+', ' ')
    
    # Checks
    allowed_kwaliteitsoordeel = ['00', '03', '04', '25', '99', 0, 3, 4, 25, 99]
    allowed_referentiehorizontaal = ['EPSG:4258', 'EPSG4258']

    # Initialize the result DataFrame
    result = pd.DataFrame()

    # Check kwaliteitsoordeel_code
    mask1 = ~df['kwaliteitsoordeel.code'].isin(allowed_kwaliteitsoordeel)
    df.loc[mask1, 'uitvalreden'] = 'vaste waarde ongeldig'
    df.loc[mask1, 'informatie'] = 'Kwaliteitsoordeel "{}" niet in (00,03,04,25,99)'.format(df.loc[mask1, 'kwaliteitsoordeel.code'])

    # Check namespace
    mask2 = df['namespace'] != 'NL80'
    df.loc[mask2, 'uitvalreden'] = 'vaste waarde ongeldig'
    df.loc[mask2, 'informatie'] = 'Namespace "{}" ongelijk aan "NL80"'.format(df.loc[mask2, 'namespace'])

    # Check referentiehorizontaal_code
    mask3 = ~df['referentiehorizontaal.code'].isin(allowed_referentiehorizontaal)
    df.loc[mask3, 'uitvalreden'] = 'vaste waarde ongeldig'
    df.loc[mask3, 'informatie'] = 'Referentiehorizontaal.code "{}" ongelijk aan "EPSG:4258"'.format(df.loc[mask3, 'referentiehorizontaal.code'])

    # Check analysecompartiment_code is not None
    mask4 = df['analysecompartiment.code'].notnull()
    df.loc[mask4, 'uitvalreden'] = 'vaste waarde ongeldig'
    df.loc[mask4, 'informatie'] = 'analysecompartiment_code is niet leeg'

    # Collect all invalid rows
    invalid_rows = df[df['uitvalreden'].notnull()]

    # Prepare result DataFrame
    validatie_resultaat = pd.DataFrame()
    if len(invalid_rows) != 0:
        result = pd.DataFrame({
            'databundelcode': package_name,
            'record_id': invalid_rows['meetwaarde.lokaalid'].replace('NL80_', ''),
            'uitvalreden': invalid_rows['uitvalreden'],
            'informatie': invalid_rows['informatie']
        })

        validatie_resultaat = pd.concat([validatie_resultaat, result], ignore_index=True)

    return validatie_resultaat

def regel_check(merged_df):

    # Define the condition to find records with no validation rule
    no_validation = merged_df[merged_df['validatieregel'].isnull()]
    print(no_validation)
    validatie_resultaat = pd.DataFrame()
    if len(no_validation) != 0:
        result = pd.DataFrame({
            'databundelcode': no_validation['databundelcode'],
            'record_id': no_validation['record_id'],
            'uitvalreden': 'geen validatieregel',
            'informatie': 'geen enkele validatieregel van toepassing'
        })

        validatie_resultaat = pd.concat([validatie_resultaat, result], ignore_index=True)

    return validatie_resultaat

def date_range_check(df, validatielijst, package_name):
    package_name = package_name.replace('+', ' ')

    validatie_resultaat = pd.DataFrame()

    # Get validation rules for the current databundelcode
    rules = validatielijst[validatielijst['databundelcode'].apply(lambda x: package_name.startswith(x))]

    # TODO: Check if validatie regel is null. If so skip this step

    # Find the min and max for startdate and enddate
    min_startdate = pd.to_datetime(rules['startdatum'], format='mixed').min()
    max_enddate = pd.to_datetime(rules['einddatum'], format='mixed').max()

    df['begindatum'] = pd.to_datetime(df['begindatum'], format='mixed')

    # Check if begindatum in df is between min_startdate and max_enddate
    df_not_in_range = df[~df['begindatum'].between(min_startdate, max_enddate)]

    new_rows = []
    if len(df_not_in_range) != 0:
        # Iterate through each record in false_condition_gdf
        for index, row in df_not_in_range.iterrows():
            # Create a new row as a dictionary
            new_row = {
                'databundelcode': package_name,
                'record_id': row['meetwaarde.lokaalid'].replace('NL80_', ''),  # Remove 'NL80_'
                'uitvalreden': 'datum valt buiten bereik',
                'informatie': f"{row['begindatum'].strftime('%d-%m-%Y')} valt buiten datumbereik validatieregels ( {min_startdate.strftime('%d-%m-%Y')} tm {max_enddate.strftime('%d-%m-%Y')})"
            }
            
            # Append the new row to the validatie_resultaat DataFrame
            new_rows.append(new_row)
    
    new_rows_df = pd.DataFrame(new_rows)
    validatie_resultaat = pd.concat([validatie_resultaat, new_rows_df], ignore_index=True)

    return validatie_resultaat

def other_checks(df, validatielijst, package_name):

    package_name = package_name.replace('+', ' ')

    validatie_resultaat = pd.DataFrame()

    # Get validation rules for the current databundelcode
    rules = validatielijst[validatielijst['databundelcode'].apply(lambda x: package_name.startswith(x))]

    # Validation: Missing numeric and alphanumeric values
    missing_values = df[
        (df['numeriekewaarde'].isnull()) & 
        (df['alfanumeriekewaarde'].isnull())
    ].copy()

    # Validation: Invalid limit symbols
    invalid_symbols = df[
        (df['limietsymbool'].notnull()) & 
        (~df['limietsymbool'].isin(['<', '>']))
    ].copy()

    new_rows = []
    if len(missing_values) != 0:
        # Iterate through each record in false_condition_gdf
        for index, row in missing_values.iterrows():
            # Create a new row as a dictionary
            new_row = {
                'databundelcode': package_name,
                'record_id': row['meetwaarde.lokaalid'].replace('NL80_', ''),  # Remove 'NL80_'
                'uitvalreden': 'waarde ontbreekt',
                'informatie': 'numerieke EN alfanumerieke waarde zijn leeg'
            }
            
            # Append the new row to the validatie_resultaat DataFrame
            new_rows.append(new_row)

    if len(invalid_symbols) != 0:
        # Iterate through each record in false_condition_gdf
        for index, row in invalid_symbols.iterrows():
            # Create a new row as a dictionary
            new_row = {
                'databundelcode': package_name,
                'record_id': row['meetwaarde.lokaalid'].replace('NL80_', ''),  # Remove 'NL80_'
                'uitvalreden': 'limietsymbool ongeldig',
                'informatie': 'limietsymbool dient leeg te zijn of < of >'
            }
            
            # Append the new row to the validatie_resultaat DataFrame
            new_rows.append(new_row)
    
    new_rows_df = pd.DataFrame(new_rows)
    validatie_resultaat = pd.concat([validatie_resultaat, new_rows_df], ignore_index=True)

    return validatie_resultaat

def determine_rule(df, validatielijst, package_name, group):

    regel_resultaat = pd.DataFrame()

    package_name = package_name.replace('+', ' ')

    validatieregels = validatielijst[validatielijst['databundelcode'].apply(lambda x: package_name.startswith(x))]

    df['parameter'] = df.apply(
        lambda row: row['biotaxon.naam'] if pd.notnull(row['biotaxon.naam']) and pd.isnull(row['parameter.code']) 
        else (row['parameter.code'] if pd.notnull(row['parameter.code']) and pd.isnull(row['biotaxon.naam']) 
            else np.nan),
        axis=1
    )

    df['record_id'] = df['meetwaarde.lokaalid'].str.replace('NL80_', '')
    df['locatiecode'] = df['meetobject.lokaalid'].str.replace('NL80_', '')

    # Split 'locatiecode' column in validatieregels into lists and explode
    validatieregels["locatiecode"] = validatieregels["locatiecode"].str.split(";")
    validatieregels = validatieregels.explode("locatiecode")
    
    new_rows = []
    if len(df) != 0:
        for _, row in df.iterrows():

            matched_rules = []

            found_groups = group[group['parameter'] == row['parameter']]

            begindatum = pd.to_datetime(row.get('begindatum', None), errors='coerce', format='mixed')

            for index, rule in validatieregels.iterrows():

                # Convert dates using pd.to_datetime, which handles invalid dates
                startdatum = pd.to_datetime(rule.get('startdatum', None), errors='coerce', format='mixed')
                einddatum = pd.to_datetime(rule.get('einddatum', None), errors='coerce', format='mixed')

                if ((row.get('eenheid.code', '') == rule.get('eenheid_code', '') or (pd.isna(row.get('eenheid.code', '')) and pd.isna(rule.get('eenheid_code', '')))) and
                    (rule.get('groep') == row['parameter']) or (pd.isna(rule.get('groep', '')) and pd.isna(row['parameter'])) and
                    (row.get('grootheid.code', '') == rule.get('grootheid_code', '') or (pd.isna(row.get('grootheid.code', '')) and pd.isna(rule.get('grootheid_code', '')))) and
                    (row.get('typering.code', '') == rule.get('typering_code', '') or (pd.isna(row.get('typering.code', '')) and pd.isna(rule.get('typering_code', '')))) and
                    (row.get('hoedanigheid.code', '') == rule.get('hoedanigheid_code', '') or (pd.isna(row.get('hoedanigheid.code', '')) and pd.isna(rule.get('hoedanigheid_code', '')))) and
                    (row.get('monstercompartiment.code', '') == rule.get('monstercompartiment_code', '') or (pd.isna(row.get('monstercompartiment.code', '')) and pd.isna(rule.get('monstercompartiment_code', '')))) and
                    (row.get('waardebewerkingsmethode.code', '') == rule.get('waardebewerkingsmethode_code', '') or 
                    (pd.isna(row.get('waardebewerkingsmethode.code', '')) and pd.isna(rule.get('waardebewerkingsmethode_code', '')))) and
                    (row.get('locatiecode', '') in rule.get('locatiecode', '').split(';') or 
                    (pd.isna(row.get('locatiecode', '')) and pd.isna(rule.get('locatiecode', '')))) and
                    (row.get('bemonsteringsapparaat.omschrijving', '') in rule.get('bemonsteringsapparaat_omschrijving', '').split(';') or 
                    (pd.isna(row.get('bemonsteringsapparaat.omschrijving', '')) and pd.isna(rule.get('bemonsteringsapparaat_omschrijving', '')))) and
                    (row.get('orgaan.code', '') == rule.get('orgaan_code', '') or 
                    (pd.isna(row.get('orgaan.code', '')) and pd.isna(rule.get('orgaan_code', '')))) and
                    (pd.notna(begindatum) and pd.notna(startdatum) and pd.notna(einddatum) and
                        begindatum >= startdatum and begindatum <= einddatum) and
                    (pd.isna(row['biotaxon.naam']) or rule['biotaxon_of_niet'].lower() == 'j') and
                    (row.get('organisme.naam', '') == rule.get('organisme_naam', '') or 
                    (pd.isna(row.get('organisme.naam', '')) and pd.isna(rule.get('organisme_naam', ''))))):               

                    matched_rules.append(index + 2)

            uitvalreden = 0
            betreft_verzameling = 0
            matched_rule = None

            if len(matched_rules) == 0:
                uitvalreden = 5
            else:
                matched_rule = matched_rules[0]

            if len(found_groups) == 0:
                uitvalreden = 1

            if len(found_groups) > 1:
                betreft_verzameling = 1            
            
            # Create a new row as a dictionary
            new_row = {
                'databundelcode': package_name,
                'record_id': row['meetwaarde.lokaalid'].replace('NL80_', ''),  # Remove 'NL80_'
                'uitvalreden': uitvalreden,
                'mogelijke_validatieregels': list(set(matched_rules)),
                'validatieregel': matched_rule,
                'betreftverzameling': betreft_verzameling,
                'monster_identificatie': row['monster.lokaalid']
            }
            
            # Append the new row to the validatie_resultaat DataFrame
            new_rows.append(new_row)
    
    new_rows_df = pd.DataFrame(new_rows) if new_rows else pd.DataFrame(columns=[
        'databundelcode', 'record_id', 'uitvalreden', 'mogelijke_validatieregels', 'validatieregel', 'betreftverzameling', 'monster_identificatie'
    ])

    regel_resultaat = pd.concat([regel_resultaat, new_rows_df], ignore_index=True)

    #regel_resultaat.to_csv('regelbepalen.csv')
    
    return regel_resultaat

def tellingen(df, validatielijst, package_name, rules, group):

    regel_resultaat = pd.DataFrame()

    package_name = package_name.replace('+', ' ')
    validatie_regels = validatielijst[validatielijst['databundelcode'].apply(lambda x: package_name.startswith(x))]
    validatie_regels["locatiecode"] = validatie_regels["locatiecode"].str.split(";")
    validatie_regels = validatie_regels.explode("locatiecode")

    df['cleaned_lokaalid'] = df['monster.lokaalid'].str.replace('NL80_', '')
    df['cleaned_meetwaarde_lokaalid'] = df['meetwaarde.lokaalid'].str.replace('NL80_', '')
    df['locatiecode'] = df['meetobject.lokaalid'].str.replace('NL80_', '')
    df['recordnr_monster'] = df['cleaned_meetwaarde_lokaalid'].rank(method='dense').astype(int)

    validatie_regels.index = validatie_regels.index + 2

    filtered_rules = rules.dropna(subset=['validatieregel'])

    # Now merge regelbepalen with validatieregel using 'validatieregel' column as the key
    merged = filtered_rules.merge(validatie_regels, left_on='validatieregel', right_index=True, how='inner')

    merged_with_df = merged.merge(df, left_on='record_id', right_on='cleaned_meetwaarde_lokaalid')
    #merged_with_df.to_csv('m.csv')

    # Group the dataframe by 'recordnr_monster'
    grouped = merged_with_df.groupby(["validatieregel", "databundelcode_x", "locatie.code", "locatiecode_x"])

    new_rows = []

    # Iterate over each group
    for recordnr_monster, group in grouped:

        aantal_dat = group.shape[0]  # Number of rows for the current recordnr_monster
        aantal_val = group['aantal'].iloc[0]
        limiet = group['limiet'].iloc[0]

        # Fetch the validation rule safely
        validatieregel_id = int(group['validatieregel'].iloc[0])
        if validatieregel_id in validatie_regels.index:
            validatieregel = validatie_regels.loc[validatieregel_id].to_dict()
        else:
            validatieregel = {}  # Empty dict if no rule is found


        if recordnr_monster == 0:
            soort = "tijdwaarden"
        else:
            soort = "monsters"

        if limiet == "<=" and aantal_dat > aantal_val:
            uitvalreden = f"aantal {soort} groter dan verwacht"
        elif limiet == ">=" and aantal_dat < aantal_val:
            uitvalreden = f"aantal {soort} kleiner dan verwacht"
        elif limiet == "=" and aantal_dat != aantal_val:
            uitvalreden = f"aantal {soort} ongelijk aan verwachting"
        else:
            uitvalreden = ""

        new_row = {
            'databundelcode': package_name,
            'record_id': group["record_id_x"].iloc[0],
            'locatiecode_aantal': group['locatie.code'].iloc[0],
            'aantaldat': aantal_dat,
            'limiet': limiet,
            'aantalval': aantal_val,
            'uitvalreden': uitvalreden,
            'recordnrs': '',
            'validatieregel': validatieregel  # Store the value instead of the Series
        }

        new_rows.append(new_row)

    new_rows_df = pd.DataFrame(new_rows) if new_rows else pd.DataFrame(columns=[
        'databundelcode', 'record_id', 'uitvalreden', 'mogelijke_validatieregels', 'validatieregel', 'betreftverzameling', 'monster_identificatie'
    ])

    regel_resultaat = pd.concat([regel_resultaat, new_rows_df], ignore_index=True)
    
    return regel_resultaat

def create_geopackage(gdf):

    # Export to GeoPackage
    dtype_mappings = {
        "meetobject.lokaalid": 'str',  # text
        "monster.lokaalid": 'str',      # text
        "meetwaarde.lokaalid": 'str',   # text
        "monstercompartiment.code": 'str',  # text
        "begindatum": 'str',            # text
        "begintijd": 'str',             # text
        "einddatum": 'str',             # text
        "eindtijd": 'str',              # text
        "tijd_utcoffset": 'str',        # text
        "typering.code": 'str',         # text
        "grootheid.code": 'str',        # text
        "parameter.code": 'str',        # text
        "parameter.omschrijving": 'str', # text
        "biotaxon.naam": 'str',         # text
        "eenheid.code": 'str',          # text
        "hoedanigheid.code": 'str',     # text
        "waardebewerkingsmethode.code": 'str',  # text
        "limietsymbool": 'str',         # text
        "numeriekewaarde": 'float',     # double precision
        "alfanumeriekewaarde": 'str',   # text
        "kwaliteitsoordeel.code": 'str', # bigint
        "orgaan.code": 'str',           # text
        "organisme.naam": 'str',        # text
        "bemonsteringsapparaat.omschrijving": 'str',  # text
        "geometriepunt.x": 'float',     # double precision
        "geometriepunt.y": 'float',     # double precision
        "referentiehorizontaal.code": 'str',  # text
        "begindiepte_m": 'str',         # text
        "einddiepte_m": 'str',          # text
        "referentievlak.code": 'str',   # text
        "bemonsteringsmethode.code": 'str',  # text
        "bemonsteringsmethode.codespace": 'str',  # text
        "waardebepalingstechniek.code": 'str',  # text
        "monprog.naam": 'str',          # text
        "krmcriterium": 'str',          # text
        "meetobject.namespace": 'str',   # text
        "levensstadium.code": 'str',    # text
        "lengteklasse.code": 'str',      # text
        "geslacht.code": 'str',          # text
        "verschijningsvorm.code": 'str', # text
        "levensvorm.code": 'str',        # text
        "waardebepalingsmethode.code": 'str',  # text
        "geom": 'object',                # GeoDataFrame geometry
        "resultaatdatum": 'str',         # Assuming it's text (could be datetime)
        "namespace": 'str',              # text
        "analysecompartiment.code": 'str' # text
    }

    # Convert data types of the columns based on the mappings
    for column, dtype in dtype_mappings.items():
        if column in gdf.columns:
            gdf[column] = gdf[column].astype(dtype)
    if is_local:
        gdf.to_file('output.gpkg', layer='krm_actuele_dataset', driver='GPKG')
    else:
        gdf.to_file('/tmp/output.gpkg', layer='krm_actuele_dataset', driver='GPKG')
    #gdf.to_file('output.gpkg', layer='krm_actuele_dataset', driver='GPKG')

def create_geodataframe(data, columns):
    # Create DataFrame with the data

    df = pd.DataFrame(data, columns=columns)

    # Convert the 'geom' column from WKT to Shapely geometries
    df['geom'] = df['geom'].apply(wkt.loads)

    # Convert to GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry='geom')

    # Define the CRS (Coordinate Reference System) if known
    gdf.set_crs(epsg=4258, inplace=True)

    return gdf

def download_file_from_s3(bucket_name, s3_file_key, local_file_path):
    """
    Download a file from an S3 bucket to local storage.

    :param bucket_name: The name of the S3 bucket.
    :param s3_file_key: The key (path) of the file in the S3 bucket.
    :param local_file_path: The local path where the file will be saved.
    """
    # Create an S3 client
    s3 = boto3.client('s3')

    try:
        # Download the file
        s3.download_file(bucket_name, s3_file_key, local_file_path)
        print(f"File {s3_file_key} has been downloaded to {local_file_path}.")
    except Exception as e:
        print(f"Error downloading file: {e}")

def upload_file_to_s3(file_name, bucket_name, s3_file_key):
    """
    Uploads a local file to an S3 bucket.

    :param file_name: Path to the local file
    :param bucket_name: Name of the S3 bucket
    :param s3_file_key: S3 object key (name of the file in S3)
    :return: True if file was uploaded, else False
    """
    try:
        s3.upload_file(file_name, bucket_name, s3_file_key)
        print(f"File {file_name} successfully uploaded to {bucket_name}/{s3_file_key}")
        return True
    except FileNotFoundError:
        print(f"File {file_name} was not found.")
        return False
    except NoCredentialsError:
        print("Credentials not available.")
        return False
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return False

def get_data_from_github(url):
    
    # Send an HTTP GET request to the URL
    http = urllib3.PoolManager()
    response = http.request('GET', url)
    
    # Convert the response content to a string and read it into a DataFrame
    csv_data = StringIO(response.data.decode('windows-1252'))
    df = pd.read_csv(csv_data, delimiter=';')

    df['new_index'] = range(1, len(df) + 1)
    
    return df

def get_shape_data_from_github(url, local_filename):
    
    # Specify the local folder where you want to save the file
    if is_local:
        local_folder = ''
    else:    
        local_folder = '/tmp'

    # Create the full path for the local file
    local_file_path = os.path.join(local_folder, local_filename)

    # Create a urllib3.PoolManager instance
    http = urllib3.PoolManager()

    # Send a GET request to the URL
    response = http.request('GET', url)

    # Check if the request was successful
    if response.status == 200:
        # Write the content to the local file
        with open(local_file_path, 'wb') as f:
            f.write(response.data)
        print(f'File downloaded and saved as: {local_file_path}')
    else:
        print(f'Failed to download file: {response.status}')

def report_tellingen(df, package_name):
    bucket_name = "krm-validatie-data-prod"

    if is_local:
        file_location = f'validatielijst_per_locatie_met_aantal_{package_name}.csv'
    else:
        file_location = f'/tmp/validatielijst_per_locatie_met_aantal_{package_name}.csv'
    
    df['validatieregel'] = df['validatieregel'].astype(str)

    # # Drop duplicate rows based on all columns
    unique_df = df.drop_duplicates()
    unique_df.to_csv(file_location)
    
    upload_file_to_s3(file_location, bucket_name, f'rapportages/validatielijst_per_locatie_met_aantal_{package_name}.csv')
  
def report_databundle(df, package_name, state):
    bucket_name = "krm-validatie-data-prod"

    response = s3.get_object(Bucket=bucket_name, Key='rapportages/akkoorddata.csv')
    csv_data = response['Body'].read().decode('utf-8')
    
    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    row = { 
            'databundelcode': package_name,
            'krmcriterium': df['krmcriterium'].values[0],
            'last_updated': current_datetime,
            'status': state
           }

    print(row)
    # Step 2: Load the CSV data into a DataFrame
    val = pd.read_csv(StringIO(csv_data), sep=';')

    if package_name in val['databundelcode'].values:
        # Update the existing record
        val.loc[val['databundelcode'] == package_name, ['krmcriterium', 'status', 'last_updated']] = row['krmcriterium'], row['status'], row['last_updated']
    else:
        # Append the new record
        new_record_df = pd.DataFrame([row])
        val = pd.concat([val, new_record_df], ignore_index=True)

    # Step 4: Write the updated DataFrame back to a CSV format
    csv_buffer = StringIO()
    val.to_csv(csv_buffer, index=False, sep=';')

    # Step 5: Upload the updated CSV back to S3
    s3.put_object(Bucket=bucket_name, Key='rapportages/akkoorddata.csv', Body=csv_buffer.getvalue())


def publish_to_sqs(queue_url, message_body, message_attributes=None, message_group_id=None, deduplication_id=str(uuid.uuid4())):
    """
    Publish a message to an SQS queue.
    
    :param queue_url: The URL of the SQS queue.
    :param message_body: The message body (dict or string).
    :param message_attributes: Optional. Dictionary of message attributes.
    :param message_group_id: Required for FIFO queues. The group ID for the message.
    :param deduplication_id: Optional. Unique deduplication ID for the message (FIFO queues).
    :return: The response from the SQS send_message call.
    """
    # Initialize SQS client
    sqs = boto3.client('sqs')
    
    # Ensure message_body is a string
    if isinstance(message_body, dict):
        message_body = json.dumps(message_body)
    
    try:
        # Prepare request parameters
        params = {
            'QueueUrl': queue_url,
            'MessageBody': message_body,
            'MessageAttributes': message_attributes or {}
        }
        
        # Include FIFO-specific parameters if applicable
        if 'fifo' in queue_url.lower():
            if not message_group_id:
                raise ValueError("MessageGroupId is required for FIFO queues.")
            params['MessageGroupId'] = message_group_id
            if deduplication_id:
                params['MessageDeduplicationId'] = deduplication_id
        
        response = sqs.send_message(**params)
        print(f"Message sent to SQS queue. Message ID: {response['MessageId']}")
        return response
    except Exception as e:
        print(f"Failed to send message to SQS: {str(e)}")
        raise

def lambda_handler(event, context):
    # Extract bucket name and file key from event (assuming the event contains this information)
    if is_local:
        bucket_name = "krm-validatie-data-prod"
        #zip_file_key = "input/RWS_2023_05+vervuiling+vis+20240702_IHM_pre_1580.zip"
        #zip_file_key = "input/RWS_2023_05+vervuiling+vis+20240702_raw.zip"
        zip_file_key = "input/RWS_2023_08+contam+en+eff+in+mar+slak+20240718_IHM_63.zip"
        #zip_file_key = "input/RWS_2023_11+plastic+in+stormvogels+AFW_organisme_locatie_XY_WBMTH.zip"
        #zip_file_key = "input/RWS_2023_11+plastic+in+stormvogels+20241108_IHM_248_AFW_PLAINDX.zip"
        #zip_file_key = "input/RWS_2022_10+zwerfvuil+op+strand+20231109.zip"
        #zip_file_key = "input/RWS_2023_11+plastic+in+stormvogels+20241108_IHM_248_AFW_date_LOC_ORG_REQ.zip"
        #zip_file_key = "input/RWS_2023_12_O2_T_sal_3_dieptes+20240712_IHM_SALNNT_T_promille_BAPPCODE_126.zip"
        #zip_file_key = "input/RWS_2023_11+plastic+in+stormvogels+20241108_IHM_248_AFW_datum_locatie_organisme.zip"
        #zip_file_key = "input/RWS_2023_06+P_N_sal_chlor 20240708_293_rev.zip"
    else:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        zip_file_key = event['Records'][0]['s3']['object']['key']

    try:
        # Call the function to extract and read the CSV file from the zip
        csv_content = extract_csv_from_zip_in_s3(bucket_name, zip_file_key)

        columns = [
            "Meetobject.LokaalID",
            "Monster.lokaalID",
            "Meetwaarde.lokaalID",
            "MonsterCompartiment.code",
            "Begindatum",
            "Begintijd",
            "Einddatum",
            "Eindtijd",
            "Tijd_UTCoffset",
            "Typering.code",
            "Grootheid.code",
            "Parameter.code",
            "Parameter.Omschrijving",
            "Biotaxon.naam",
            "Eenheid.code",
            "Hoedanigheid.code",
            "Waardebewerkingsmethode.code",
            "Limietsymbool",
            "Numeriekewaarde",
            "Alfanumeriekewaarde",
            "Kwaliteitsoordeel.code",
            "Orgaan.code",
            "Organisme.naam",
            "Bemonsteringsapparaat.Omschrijving",
            "GeometriePunt.X",
            "GeometriePunt.Y",
            "Referentiehorizontaal.code",
            "BeginDiepte_m",
            "EindDiepte_m",
            "Referentievlak.code",
            "Bemonsteringsmethode.code",
            "Bemonsteringsmethode.codespace",
            "Waardebepalingstechniek.code",
            "Monprog.naam",
            "KRMcriterium",
            "Meetobject.Namespace",
            "Levensstadium.code",
            "Lengteklasse.code",
            "Geslacht.code",
            "Verschijningsvorm.code",
            "Levensvorm.code",
            "Waardebepalingsmethode.code",
            "geom",
            "ResultaatDatum",
            "Namespace",
            "Analysecompartiment.code"
        ]

        data = []

        for index, row in csv_content.iterrows():
            geom = 'POINT(' + str(row['GeometriePunt.X']) + ' ' + str(row['GeometriePunt.Y']) + ')'
            row = [row['Meetobject.LokaalID'],
                    row['Monster.lokaalID'],
                    row['Meetwaarde.lokaalID'],
                    row['MonsterCompartiment.code'],
                    row['Begindatum'],
                    row['Begintijd'],
                    row['Einddatum'],
                    row['Eindtijd'],
                    row['Tijd_UTCoffset '],
                    row['Typering.code'],
                    row['Grootheid.code'],
                    row['Parameter.code'],
                    row['Parameter.Omschrijving'],
                    row['Biotaxon.naam'],
                    row['Eenheid.code'],
                    row['Hoedanigheid.code'],
                    row['Waardebewerkingsmethode.code'],
                    row['Limietsymbool'],
                    row['Numeriekewaarde'],
                    row['Alfanumeriekewaarde'],
                    row['Kwaliteitsoordeel.code'],
                    row['Orgaan.code'],
                    row['Organisme.naam'],
                    row['Bemonsteringsapparaat.omschrijving'],
                    row['GeometriePunt.X'],
                    row['GeometriePunt.Y'],
                    row['Referentiehorizontaal.code'],
                    row['BeginDiepte_m '],
                    row['EindDiepte_m '],
                    row['Referentievlak.code '],
                    row['Bemonsteringsmethode.code '],
                    row['Bemonsteringsmethode.codespace '],
                    row['Waardebepalingstechniek.code'],
                    databundel_code,
                    'ANSNL-D1C2',
                    row['Meetobject.Namespace'],
                    row['Levensstadium.code'],
                    row['Lengteklasse.code'],
                    row['Geslacht.code'],
                    row['Verschijningsvorm.code'],
                    row['Levensvorm.code'],
                    row['Waardebepalingsmethode.code'],
                    geom,
                    row['ResultaatDatum'],
                    row['Namespace'],
                    row['Analysecompartiment.code']
                ]
            
            data.append(row)

        # print(data)
        package_name = os.path.splitext(os.path.basename(zip_file_key))[0]
        #print(package_name)
        gdf = create_geodataframe(data, columns)

        gdf = gdf.rename(columns=str.lower)

        df = validate_records(gdf, package_name)

        # Create a geopackage out of the data bundle
        df.drop(columns=['resultaatdatum', 'namespace', 'analysecompartiment.code'], inplace=True)

        if bundel_akkoord or akkoord_file:
            create_geopackage(df)

            report_databundle(df, package_name, f"Databundel validatie is: {bundel_akkoord} en akkoord file is: {akkoord_file}")

            print("Uploading!")

            if not is_local:
                upload_file_to_s3('/tmp/output.gpkg', 'krm-validatie-data-prod' ,'geopackages/' + package_name + '.gpkg')

                queue_url = "https://sqs.eu-west-1.amazonaws.com/637423531264/publishToTest.fifo"

                publish_to_sqs(
                    queue_url=queue_url,
                    message_body="test",
                    message_attributes=None,
                    message_group_id='example_group_id'
                )
        else:
            report_databundle(df, package_name, f"Databundel validatie is: {bundel_akkoord} en akkoord file is: {akkoord_file}")

        return {
            'statusCode': 200,
            'message': 'CSV file found and read successfully'
        }
    
    except Exception as e:
        print(str(e))
        return {
            'statusCode': 500,
            'message': str(e)
        }

lambda_handler('', '')