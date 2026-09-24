# Downloading Waterinfo data — user guide

This guide is for people who **start the download from the AWS web console** and **change the
settings on GitHub**. You do not need to install anything, and you do not need to write code.
A browser is enough.

If you are looking for the technical documentation (running it locally with Docker, rebuilding
the Lambda layer, Terraform), see [`README.md`](README.md) instead.

## What this does

A small program on AWS, called `krm-downloading-lambda-dev`, downloads measurement data from
Rijkswaterstaat Waterinfo and saves it as CSV files in the project's data bucket.

It works in three steps:

1. You **decide what to download** by editing a settings file on GitHub.
2. You **start the download** by sending a message from the AWS console.
3. The program **saves the CSV files** in the `downloaded/` folder of the S3 bucket.

Each run takes roughly half a minute for the standard settings, and longer for larger periods
or more locations.

---

## Part 1 — Change the settings on GitHub

All settings live in one file:

**<https://github.com/openearth/krmvalidatie/blob/main/data/waterinfo_downloading_settings.toml>**

This is the same `data/` folder that holds `validatielijst.csv` and the other lookup tables, so
it works exactly like updating one of those.

### Steps

1. Open the link above and make sure you are logged in to GitHub.
2. Click the **pencil icon** (✏️ *Edit this file*) at the top right of the file.
3. Change the values you need — see the table below for what each one means.
4. Scroll down to **Commit changes**.
5. Write a short description of what you changed, for example
   *"Download WATHTE for January 2024"*.
6. Choose **Commit directly to the `main` branch** and click **Commit changes**.

That is all. The program reads this file from GitHub every time it runs, so your change is
active immediately — there is nothing to deploy or restart.

### What you can change

| Setting | Example | What it means |
| --- | --- | --- |
| `grootheid_code` | `"AANTPLTE"` | Which quantity to download. This is the main choice. |
| `start_date` | `"2022-12-31"` | First day of the period, always `YYYY-MM-DD`. |
| `end_date` | `"2023-12-31"` | Last day of the period, always `YYYY-MM-DD`. |
| `compartiment_code` | `"OR"` | Compartment the measurement belongs to. |
| `method_code` | `"other:X156"` | Selects which locations are used. Leave as is unless you know you need another set. |
| `limit_locations` | `0` | `0` downloads all locations. Set it to `1` or `2` for a quick test run. |
| `eenheid_code` | `""` | Unit code. Usually left empty. |
| `meetapparaat_code` | `""` | Measuring device code. Usually left empty. |
| `max_workers` | `8` | How many downloads run at the same time. Leave as is. |
| `s3_prefix` | `"downloaded"` | The folder in the bucket where results are saved. |
| `output_dir` | `"waterinfo"` | Only used when someone runs the script on their own laptop. Ignored on AWS. |

### Rules to avoid mistakes

- Keep the **quotation marks** around text values: `grootheid_code = "WATHTE"`, not
  `grootheid_code = WATHTE`.
- Do **not** put quotation marks around numbers: `limit_locations = 2`, not `"2"`.
- Dates are always `YYYY-MM-DD`, so 1 March 2024 is `2024-03-01`.
- Lines starting with `#` are explanations. You can change them or leave them alone; the program
  ignores them.
- Do not rename the settings themselves, only the values after the `=` sign.

> 💡 **Tip:** if you are unsure, set `limit_locations = 1` first. The run then downloads only one
> location, finishes in a few seconds, and lets you check the result before doing the full run.
> Remember to set it back to `0` afterwards.

---

## Part 2 — Start the download from the AWS console

1. Log in to the AWS console and make sure the region at the top right is
   **Europe (Ireland) / eu-west-1**. The download will not appear if you are in another region.
2. Search for **SNS** in the top search bar and open **Simple Notification Service**.
3. In the left-hand menu click **Topics**.
4. Click the topic named **`DownloadWaterinfo-dev`**.
5. Click the orange **Publish message** button at the top right.
6. Fill in the form:
   - **Subject** — optional, you can leave it empty or write something like `Manual download`.
   - **Message body** — choose *Identical payload for all delivery protocols* and type exactly:

     ```
     {}
     ```

     Those two curly brackets mean *"use the settings from the GitHub file as they are"*. This is
     the normal case.
7. Leave everything else unchanged and click **Publish message** at the bottom.

The download starts within a few seconds. The console does not show the progress — see Part 4 to
follow along or to check that it worked.

### Optional: change something for one run only

If you want to deviate just once without editing GitHub, you can put the settings in the message
body instead of `{}`. For example, to download a different quantity for a different period:

```
{"grootheid_code": "WATHTE", "start_date": "2024-01-01", "end_date": "2024-02-01"}
```

Anything you do not mention keeps the value from the GitHub file. Watch the format: double
quotation marks around names and text, a colon between name and value, a comma between entries,
and curly brackets around the whole thing.

You can override: `grootheid_code`, `start_date`, `end_date`, `compartiment_code`,
`eenheid_code`, `meetapparaat_code`, `method_code`, `max_workers`, `limit_locations` and
`s3_prefix`.

> ⚠️ Use this for one-off runs. Because it is not written down anywhere, nobody else can see what
> was used. For anything you want to repeat or share, edit the GitHub file instead.

---

## Part 3 — Find your results

1. Search for **S3** in the console and open it.
2. Click the bucket **`krm-validatie-data-dev`**.
3. Open the folder **`downloaded/`**, then the folder named after the quantity you downloaded,
   for example `AANTPLTE/`.

You will find:

```
downloaded/
└── AANTPLTE/
    ├── bergenaanzee.standafvalmeetnet.csv      one file per location
    ├── noordwijk.strandafvalmeetnet.csv
    ├── terschelling.strandafvalmeetnet.csv
    ├── veere.strandafvalmeetnet.csv
    └── AANTPLTE_combined.csv                   all locations together
```

The `_combined.csv` file is usually the one you want. To download it, tick the checkbox next to
it and click **Download**.

Files from a new run **replace** the files of the previous run for the same quantity. Results for
different quantities are kept in separate folders and do not overwrite each other.

---

## Part 4 — Check whether it worked

1. Search for **Lambda** in the console and open it.
2. Click the function **`krm-downloading-lambda-dev`**.
3. Open the **Monitor** tab and click **View CloudWatch logs**.
4. Click the newest log stream at the top of the list.

You will see lines like:

```
Reading settings from https://raw.githubusercontent.com/openearth/krmvalidatie/...
1268 catalog records, 4 unique locations
Processing location: bergenaanzee.standafvalmeetnet
Uploaded 584 rows to s3://krm-validatie-data-dev/downloaded/AANTPLTE/bergenaanzee.standafvalmeetnet.csv
...
Uploaded 2336 rows to s3://krm-validatie-data-dev/downloaded/AANTPLTE/AANTPLTE_combined.csv
{"bucket": "krm-validatie-data-dev", "prefix": "downloaded/AANTPLTE", "locations": 4,
 "downloaded": 4, "failed": [], "rows": 2336}
```

That last line is the summary of the run. `"downloaded"` is the number of locations that
produced data, `"failed"` lists the ones that did not, and `"rows"` is the size of the combined
file. If `"failed"` is empty and `"rows"` is greater than zero, everything went well.

### If something goes wrong

| What you see | What it usually means | What to do |
| --- | --- | --- |
| No new log stream appears | The message was never published, or you are in the wrong region | Check the region is eu-west-1 and publish again |
| An error right after `Reading settings from ...` | The settings file has a typo and can no longer be read | Open the file on GitHub and check the quotation marks and `=` signs of your last change |
| `0 catalog records, 0 unique locations` | The `method_code` matches nothing in the catalog | Set `method_code` back to `other:X156` |
| `no data returned` per location, or `no data downloaded` | Waterinfo has no data for that quantity, period or location | Try a different period, or check the code on the Waterinfo site |
| `Message is not JSON, ignoring it` | A one-off override was typed incorrectly; the GitHub settings were used instead | Check the brackets, quotation marks and commas in the message body |
| `Ignoring unknown setting in message` | A setting name in the message is misspelled or cannot be overridden | Check the spelling against the list of overridable settings above |
| `Task timed out` | The requested period is too large to finish in one run | Split it into shorter periods and run them one after another |

If it still fails, copy the last lines of the log and pass them on to the development team —
that is usually enough for them to see what happened.

---

## Good to know

- The download can also run **automatically on a schedule**, but this is currently switched off.
  The development team can enable it if you want a nightly run.
- Running it more than once is harmless. It only reads from Waterinfo and rewrites the CSV files.
- `dev` is the test environment. Once everything works there, the development team moves the same
  setup to production.
- Everything you change on GitHub is recorded in the file history, so you can always see who
  changed which setting and when, and go back to an earlier version.

## Quick reference

| | |
| --- | --- |
| Region | Europe (Ireland) — eu-west-1 |
| Settings file | `data/waterinfo_downloading_settings.toml` on GitHub `main` |
| SNS topic to publish to | `DownloadWaterinfo-dev` |
| Standard message body | `{}` |
| Results bucket | `krm-validatie-data-dev` |
| Results folder | `downloaded/<grootheid_code>/` |
| Lambda function (logs) | `krm-downloading-lambda-dev` |
