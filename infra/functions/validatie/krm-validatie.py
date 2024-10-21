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

s3 = boto3.client('s3')
databundel_code = ""

def extract_csv_from_zip_in_s3(bucket_name, zip_file_key):
    try:
        # Download the zip file from S3
        decoded_key = unquote_plus(zip_file_key)

        databundel_code = decoded_key.split('/')[-1]
        
        zip_obj = s3.get_object(Bucket=bucket_name, Key=decoded_key)
        zip_data = zip_obj['Body'].read()

        # Open the zip file in memory
        with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
            file_list = z.namelist()  # Get a list of all the files in the zip

            # Iterate through the files to find a CSV file
            for file_name in file_list:
                if file_name.endswith('.csv'):
                    # Found a CSV file, open and read it
                    with z.open(file_name) as csvfile:
                        # Read the CSV file in memory
                        with io.TextIOWrapper(csvfile, encoding='cp1252') as textfile:
                            csv_content = pd.read_csv(textfile, delimiter=';')
                    break

            if not csv_content.empty:
                return csv_content
            else:
                return None

    except Exception as e:
        raise RuntimeError(f"Error extracting or reading CSV from zip in S3: {str(e)}")

def validate_records(gdf, package_name):

    validatielijst = get_data_from_github('https://krm-validatie-data-floris.s3.eu-west-1.amazonaws.com/validatie_data/validatielijst.csv')
    group = get_data_from_github('https://krm-validatie-data-floris.s3.eu-west-1.amazonaws.com/validatie_data/groep.csv')

    #column_definition = pd.read_csv('kolomdefinitie.csv', delimiter=';')
    column_definition = get_data_from_github('https://krm-validatie-data-floris.s3.eu-west-1.amazonaws.com/validatie_data/kolomdefinitie.csv')

    # geocontrole (validatie.locatie)
    geo_control_result = geocontrol(gdf)

    # verplichte kolommen check (validatie.verplichtekolom)
    column_check_result = column_check(gdf, validatielijst, package_name, column_definition)
    print(column_check_result)
    # kolomwaarde-controle (validatie.kolomwaarde)
    column_value_result = column_value_check(gdf, validatielijst, package_name)

    # # aantal controle (validatie.aantal)
    # count_check()

    rules = determine_rule(gdf, validatielijst, package_name, group)

    # # parameter controle (validatie.parameter)
    #parameter_check_result = parameter_check(rules)

    # # parameter verzameling (validatie.parameterverzameling)
    parameter_aggregate_result =  parameter_aggregate(gdf, rules)
    
    # # vaste waarden controle (validatie.vastewaarde)
    value_check_result = value_check(gdf)

    # # vangnet (validatie.regelcontrole)
    regel_check_result = regel_check(rules)

    # # controle datumbereik (validatie.datumbereik)
    result_date_range_check = date_range_check(gdf, validatielijst, package_name)
   
    # Create the Markdown report
    markdown_report = []

    # Add Title
    markdown_report.append("# Validation Report\n")
    markdown_report.append("This report contains the results of the validation checks.\n")

    # Add Section for Regel Check
    markdown_report.append("## 1. Geo controle\n")
    markdown_report.append(geo_control_result.to_markdown(index=False))
    markdown_report.append("\n")  # Add a newline for better readability

    # Add Section for Date Range Check
    markdown_report.append("## 2. Verplichte kolommen controle\n")
    markdown_report.append(column_check_result.to_markdown(index=False))
    markdown_report.append("\n")  # Add a newline for better readability

    # Add Section for Regel Check
    markdown_report.append("## 3. Kolom waarden controle\n")
    markdown_report.append(column_value_result.to_markdown(index=False))
    markdown_report.append("\n")  # Add a newline for better readability

    # # Add Section for Date Range Check
    # markdown_report.append("## 4. Parameter controle\n")
    # markdown_report.append(parameter_check_result.to_markdown(index=False))
    # markdown_report.append("\n")  # Add a newline for better readability

    # Add Section for Regel Check
    markdown_report.append("## 5. Parameter verzameling controle\n")
    markdown_report.append(parameter_aggregate_result.to_markdown(index=False))
    markdown_report.append("\n")  # Add a newline for better readability

    # Add Section for Date Range Check
    markdown_report.append("## 6. Vaste waarden controle\n")
    markdown_report.append(value_check_result.to_markdown(index=False))
    markdown_report.append("\n")  # Add a newline for better readability

    # Add Section for Regel Check
    markdown_report.append("## 7. Validatie data zonder validatieregel\n")
    markdown_report.append(regel_check_result.to_markdown(index=False))
    markdown_report.append("\n")  # Add a newline for better readability

    # Add Section for Date Range Check
    markdown_report.append("## 8. Datumbereik controle\n")
    markdown_report.append(result_date_range_check.to_markdown(index=False))
    markdown_report.append("\n")  # Add a newline for better readability

    # Combine everything into a single string
    final_report = "\n".join(markdown_report)

    # Write to a .md file
    filename = f'/tmp/{databundel_code}.md'
    # filename = f'{package_name}.md'
    with open(filename, 'w') as f:
        f.write(final_report)
    
    #upload_file_to_s3(filename, 'krm-validatie-data-prod', f'rapportages/{filename}')

    return True

def geocontrol(df):
    print("GeoControl")
    
    # Read shapefile
    # TODO: combine shapefile in zip and extract it
    shapefile_path = 'shapes/'
    download_file_from_s3('krm-validatie-data-floris', shapefile_path + 'KRM2_P.shp', '/tmp/KRM2_P.shp')
    download_file_from_s3('krm-validatie-data-floris', shapefile_path + 'KRM2_P.shx', '/tmp/KRM2_P.shx')
    download_file_from_s3('krm-validatie-data-floris', shapefile_path + 'KRM2_P.prj', '/tmp/KRM2_P.prj')
    download_file_from_s3('krm-validatie-data-floris', shapefile_path + 'KRM2_P.dbf', '/tmp/KRM2_P.dbf')
    download_file_from_s3('krm-validatie-data-floris', shapefile_path + 'KRM2_P.cpg', '/tmp/KRM2_P.cpg')

    # Convert the coordinates from the array into shapely Points
    points_from_array = [Point(xy) for xy in zip(df['geometriepunt.x'], df['geometriepunt.y'])]

    gdf = gpd.read_file('/tmp/KRM2_P.shp')

    mpindents = []

    # Access the geometry and attributes of the data points
    for index, row in gdf.iterrows():
        geometry = row['geometry']  # The geometry of the feature
        attributes = row.drop('geometry')  # All other attributes excluding geometry
        mpindents.append(attributes.MPNIDENT)

    #print(mpindents)
    # Create a GeoDataFrame for points from the array
    gdf_array = gpd.GeoDataFrame(df, geometry=points_from_array, crs="EPSG:4258")

    gdf_array['Meetobject.lokaalID_cleaned'] = gdf_array['meetobject.lokaalid'].str.replace('NL80_', '', regex=False)

    # Check if 'Meetobject.lokaalID_cleaned' is in the array
    mask = gdf_array['Meetobject.lokaalID_cleaned'].isin(mpindents)

    # Filter the rows where the condition is true
    filtered_gdf = gdf_array[mask]

    # Buffer the shapefile points by 100 meters to create a 100m radius around each point
    gdf['buffer'] = gdf.geometry.buffer(0.002)  # Approx 0.001 degrees is ~100 meters at equator

    # Check if any points from the array are within 100m of the shapefile points
    result = gdf_array[gdf_array.geometry.apply(lambda x: gdf['buffer'].contains(x).any())]

    # Initialize the result DataFrame
    results = pd.DataFrame()

    if len(filtered_gdf) != len(df):
        results["message"] = "Points outside boundary"

    if len(result) != len(df):
        results["message"] = "Points outside boundary"
    
    if not result.empty:
        results["message"] = "Points within 100 meters:"
    else:
        results["message"] = "No points within 100 meters."

    return results

def column_check(df, validatielijst, package_name, column_definition):
    print('ColumnCheck')
    package_name = package_name.replace('+', ' ')
    print(validatielijst)
    filtered_validatielijst = validatielijst[validatielijst['databundelcode'].apply(lambda x: package_name.startswith(x))]

    if len(filtered_validatielijst) == 0:
        print('Databundel niet gevonden in validatielijst')
        return False

    mandatory_columns = column_definition[column_definition['ihm_verplicht'] == 'V']['kolomnaam'].str.lower()
    empty_columns = df[mandatory_columns].isna().any()

    # Filter and print the columns that have empty values
    columns_with_nan = empty_columns[empty_columns].index.tolist()

    # Initialize the result DataFrame
    results = pd.DataFrame()

    if columns_with_nan:
        print("Columns with empty values:", columns_with_nan)
        results = pd.concat([results, columns_with_nan], ignore_index=True)
    else:
        print("All rows have been filled in these columns.")
        results["message"] = "All rows have been filled in these columns."
    
    return results


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
    print('ColumnValueCheck')
    results = []
    
    package_name = package_name.replace('+', ' ')

    # Get validation rules for the current databundelcode
    rules = validatielijst[validatielijst['databundelcode'].apply(lambda x: package_name.startswith(x))]

    df['locatie.code'] = df['meetobject.lokaalid'].str.replace('NL80_', '', regex=False)

    for _, row in df.iterrows():
        record_results = {
            'databundelcode': databundel_code,
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
        #print(row['grootheid.code'])
        # Check against validation rules
        for _, rule in rules.iterrows():
            if not (pd.isna(row['grootheid.code']) and pd.isna(rule['grootheid_code'])) and row['grootheid.code'] != rule['grootheid_code']:
                mismatch_counts['grootheid'] += 1
            if not (pd.isna(row['typering.code']) and pd.isna(rule['typering_code'])) and row['typering.code'] != rule['typering_code']:
                mismatch_counts['typering'] += 1
            if not (pd.isna(row['eenheid.code']) and pd.isna(rule['eenheid_code'])) and row['eenheid.code'] != rule['eenheid_code']:
                mismatch_counts['eenheid'] += 1
            if not (pd.isna(row['hoedanigheid.code']) and pd.isna(rule['hoedanigheid_code'])) and row['hoedanigheid.code'] != rule['hoedanigheid_code']:
                mismatch_counts['hoedanigheid'] += 1
            if not (pd.isna(row['waardebewerkingsmethode.code']) and pd.isna(rule['waardebewerkingsmethode_code'])) and row['waardebewerkingsmethode.code'] != rule['waardebewerkingsmethode_code']:
                mismatch_counts['waardebewerkingsmethode'] += 1
            if not (pd.isna(row['monstercompartiment.code']) and pd.isna(rule['monstercompartiment_code'])) and row['monstercompartiment.code'] != rule['monstercompartiment_code']:
                mismatch_counts['compartimentcode'] += 1
            if not (pd.isna(row['locatie.code']) and pd.isna(rule['locatiecode'])) and not rule['locatiecode'].find(row['locatie.code']):
                mismatch_counts['locatiecode'] += 1
            if not (pd.isna(row['bemonsteringsapparaat.omschrijving']) and pd.isna(rule['bemonsteringsapparaat_omschrijving'])) and row['bemonsteringsapparaat.omschrijving'] != rule['bemonsteringsapparaat_omschrijving']:
                mismatch_counts['veldapparaatomschrijving'] += 1
            if not (pd.isna(row['organisme.naam']) and pd.isna(rule['organisme_naam'])) and row['organisme.naam'] != rule['organisme_naam']:
                mismatch_counts['organismenaam'] += 1
            
        # Determine uitvalreden and informatie
        total_rules = len(rules)
        if mismatch_counts['grootheid'] == total_rules:
            record_results['uitvalreden'] = 'ongeldige code'
            record_results['informatie'] = f"Grootheid.code '{row['grootheid.code']}' niet in: {{{','.join(rules['grootheid_code'])}}}."
        elif mismatch_counts['typering'] == total_rules:
            record_results['uitvalreden'] = 'ongeldige code'
            record_results['informatie'] = f"Typering.code '{row['typering.code']}' niet in: {{{','.join(rules['typering_code'])}}}."        
        elif mismatch_counts['eenheid'] == total_rules:
            record_results['uitvalreden'] = 'ongeldige code'
            record_results['informatie'] = f"Eenheid.code '{row['eenheid.code']}' niet in: {{{','.join(rules['eenheid_code'])}}}."
        elif mismatch_counts['hoedanigheid'] == total_rules:
            record_results['uitvalreden'] = 'ongeldige code'
            record_results['informatie'] = f"Hoedanigheid.code '{row['hoedanigheid.code']}' niet in: {{{','.join(rules['hoedanigheid_code'])}}}."
        elif mismatch_counts['waardebewerkingsmethode'] == total_rules:
            record_results['uitvalreden'] = 'ongeldige code'
            record_results['informatie'] = f"Waardebewerkingsmethode '{row['waardebewerkingsmethode.code']}' niet in: {{{','.join(rules['waardebewerkingsmethode_code'])}}}."
        elif mismatch_counts['compartimentcode'] == total_rules:
            record_results['uitvalreden'] = 'ongeldige code'
            record_results['informatie'] = f"Compartimentcode '{row['monstercompartiment.code']}' niet in: {{{','.join(rules['monstercompartiment_code'])}}}."
        elif mismatch_counts['locatiecode'] == total_rules:
            record_results['uitvalreden'] = 'ongeldige code'
            record_results['informatie'] = f"Locatiecode '{row['locatie.code']}' niet in: {{{','.join(rules['locatiecode'])}}}."
        elif mismatch_counts['veldapparaatomschrijving'] == total_rules:
            record_results['uitvalreden'] = 'ongeldige code'
            record_results['informatie'] = f"Veldapparaatomschrijving '{row['bemonsteringsapparaat.omschrijving']}' niet in: {{{','.join(rules['bemonsteringsapparaat_omschrijving'])}}}."
        elif mismatch_counts['organismenaam'] == total_rules:
            record_results['uitvalreden'] = 'ongeldige code'
            record_results['informatie'] = f"Organismenaam '{row['organisme.naam']}' niet in: {{{','.join(rules['organisme_naam'])}}}."
        # else:
        #     record_results['uitvalreden'] = 'ongeldige combinatie'
        #     record_results['informatie'] = 'deze monster-tijdwaarde past niet in de validatielijst van deze bundel'
        # Add more conditions for other columns as necessary
        
        results.append(record_results)

    results_df = pd.DataFrame(results)

    return results_df

    # (1) als een kolomwaarden met geen enkele van de waarden uit de validatieregels correspondeert, dan is die waarde ongeldig

    # (2) als een kolomwaarde voorkomt bij tenminste één validatieregel (en (1) is niet van toepassing), dan is de combinatie van kolomwaarden niet geldig (en wellicht één van de kolomwaarden fout)
    print('deze monster-tijdwaarde past niet in de validatielijst van deze bundel')

    return True

def count_check():
    ''' 
        uitgangspunten:
        - de kolom recordnr maakt een datarecord uniek
        - er is voor een datarecord precies één match met de tabel validatieregels
        - de validatieregels zijn te matchen op deze kolommen: eenheid,grootheid,typering,hoedanigheid,waardebewerkingsmethode,locatiecode
        - wanneer er een match is kan er gecontroleerd worden op aantal (i.c.m. limiet en datumbereik)
        - als er een monster is: aantal monsters tellen (dat.monster_lokaalid)
        - als er geen monster is: aantal tijdwaarden tellen
	'''

    return True

def parameter_check(merged_df):
    print('ParameterCheck')
     # Step 2: Filter based on uitvalreden
    filtered_df = merged_df[merged_df['uitvalreden'].isin([1, 2, 3])]

    # Step 3: Create Result DataFrame
    results = filtered_df[['databundelcode']].copy()
    results['uitvalreden'] = 'parameter is ongeldig'
    results['informatie'] = (
        'parameter "' + results['parameter'].fillna('') + '" i.c.m. groep (' + 
        filtered_df['groep'].str.lower() + ') uit de validatieregel komt niet voor in de groep-lijst'
    )

    return results

def parameter_aggregate(df, merged_df):
    # Ensure the parameter names are in lower case for consistency
    print('ParameterAggregate')
    df['parameter'] = df['parameter'].str.lower()

    # Initialize the result DataFrame
    results = pd.DataFrame()

    # Group the validation rules to get expected parameters
    expected_parameters = (
        merged_df[
            (merged_df['betreftverzameling'] == 1) &
            (merged_df['uitvalreden'] == 0)
        ]
        .groupby(['groep_x', 'parameter'], as_index=False)
        .agg({
            'databundelcode': 'min'
        })
    )

    # Merge expected parameters with the data to find missing ones
    merged = expected_parameters.merge(
        df[['parameter']], on='parameter', how='left', indicator=True
    )

    # Find missing parameters
    missing_params = merged[merged['_merge'] == 'left_only']

    # Create results DataFrame
    results = missing_params.assign(
        databundelcode=databundel_code,
        uitvalreden='ontbrekende parameter',
        informatie=lambda x: 'parameter "{}" uit groep "{}" niet gevonden'.format(
            x['parameter'], x['groep_x']
        )
    )[['databundelcode', 'uitvalreden', 'informatie']]

    return results

def value_check(df):
    print('value_check')
    # Checks
    allowed_kwaliteitsoordeel = ['00', '03', '04', '25', '99']
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

    # Append to the result
    result = pd.concat([result, invalid_rows], ignore_index=True)

    return result

def regel_check(merged_df):
    print('RegelCheck')
    # Define the condition to find records with no validation rule
    no_validation = merged_df[merged_df['validatieregel'].isnull()]
    
    # Create results DataFrame
    results = no_validation.assign(
        databundelcode=databundel_code,
        uitvalreden='geen validatieregel',
        informatie='geen enkele validatieregel van toepassing'
    )[['databundelcode', 'uitvalreden', 'informatie']]

    return results

def date_range_check(df, validatielijst, package_name):
    print('DateRangeCheck')
    package_name = package_name.replace('+', ' ')

    # Get validation rules for the current databundelcode
    rules = validatielijst[validatielijst['databundelcode'].apply(lambda x: package_name.startswith(x))]

    # TODO: Check if validatie regel is null. If so skip this step

    # Find the min and max for startdate and enddate
    min_startdate = pd.to_datetime(rules['startdatum']).min()
    max_enddate = pd.to_datetime(rules['einddatum']).max()

    #df['begindatum'] = '2025-12-31'
    df['begindatum'] = pd.to_datetime(df['begindatum'])

    # Check if begindatum in df is between min_startdate and max_enddate
    df_not_in_range = df[~df['begindatum'].between(min_startdate, max_enddate)]

    results = (
        df_not_in_range['meetwaarde.lokaalid'] +
        df_not_in_range['begindatum'].dt.strftime('%d-%m-%Y') + 
        ' valt buiten datumbereik validatieregels (' + 
        min_startdate.strftime('%d-%m-%Y') + ' tm ' + 
        max_enddate.strftime('%d-%m-%Y') + ')'
    )

    return results

def determine_rule(df, validatielijst, package_name, group):

    df['begindatum'] = pd.to_datetime(df['begindatum'], format='%d-%m-%Y', errors='coerce')

    package_name = package_name.replace('+', ' ')
    rules = validatielijst[validatielijst['databundelcode'].apply(lambda x: package_name.startswith(x))]

    df['parameter'] = df.apply(
        lambda row: row['biotaxon.naam'] if pd.notnull(row['biotaxon.naam']) and pd.isnull(row['parameter.code']) 
        else (row['parameter.code'] if pd.notnull(row['parameter.code']) and pd.isnull(row['biotaxon.naam']) 
            else f"{row['parameter.code']} / {row['biotaxon.naam']}"),
        axis=1
    )

    df['locatie.code'] = df['meetobject.lokaalid'].str.replace('NL80_', '', regex=False)

    # Merge the two dataframes on the 'parameter' column
    merged_df = pd.merge(df, group, on='parameter', how='inner')

    # Preprocess for lower case and coalesce equivalent
    merged_df = merged_df.fillna('')
    rules = rules.fillna('')

    # Merge the DataFrames
    merged = merged_df.merge(rules, how='inner', left_on=[
        'eenheid.code', 'grootheid.code', 'typering.code', 
        'hoedanigheid.code', 'monstercompartiment.code', 
        'waardebewerkingsmethode.code', 'locatie.code', 
        'bemonsteringsapparaat.omschrijving', 'orgaan.code'
    ], right_on=[
        'eenheid_code', 'grootheid_code', 'typering_code', 
        'hoedanigheid_code', 'monstercompartiment_code', 
        'waardebewerkingsmethode_code', 'locatiecode', 
        'bemonsteringsapparaat_omschrijving', 'orgaan_code'
    ])

    # Apply conditions
    filtered = merged[
        (merged['begindatum'].between(pd.to_datetime(merged['startdatum'], format='%d-%m-%Y'),
                                    pd.to_datetime(merged['einddatum_y'], format='%d-%m-%Y'))) &
        ((merged['biotaxon.naam'].isnull()) | (merged['biotaxon_of_niet'].str.lower() == 'j')) &
        (merged['organisme.naam'].str.lower() == merged['organisme_naam'])
    ]

    result = filtered.copy()

    result['uitvalreden'] = 0
    result['mogelijke_validatieregels'] = [""]
    result['validatieregel'] = ""
    result['betreftverzameling'] = 1
    result['monster_identificatie'] = result['monster.lokaalid']

    return result

def create_geopackage(gdf):

    # Export to GeoPackage
    gdf.to_file('/tmp/output.gpkg', layer='krm_actuele_dataset', driver='GPKG')
    # gdf.to_file('output.gpkg', layer='krm_actuele_dataset', driver='GPKG')

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
    
    return df
    
def lambda_handler(event, context):
    # Extract bucket name and file key from event (assuming the event contains this information)

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    zip_file_key = event['Records'][0]['s3']['object']['key']

    #bucket_name = "krm-validatie-data-prod"
    #zip_file_key = "input/RWS_2023_05+vervuiling+vis+20240702_IHM_pre_1580.zip"

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
                    '',
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
        print(package_name)
        gdf = create_geodataframe(data, columns)

        gdf = gdf.rename(columns=str.lower)

        if validate_records(gdf, package_name):

            # Create a geopackage out of the data bundle
            gdf.drop(columns=['resultaatdatum', 'namespace', 'analysecompartiment.code'])

            create_geopackage(gdf)
            # Upload geopackage to S3/MinIO
            upload_file_to_s3('/tmp/output.gpkg', 'krm-validatie-data-prod' ,'geopackages/' + package_name + '.gpkg')

            return {
                'statusCode': 200,
                'message': 'CSV file found and read successfully'
            }
        else:
            return {
                'statusCode': 404,
                'message': 'No CSV file found in the zip archive'
            }

    except Exception as e:
        print(str(e))
        return {
            'statusCode': 500,
            'message': str(e)
        }
    

#lambda_handler('', '')