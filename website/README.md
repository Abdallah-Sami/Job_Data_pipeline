# Masar website

**TODO:** add the static site files here (`index.html` and assets) — the same files deployed to the
`$web` container of the `jobpipelinedatalake` storage account.

The site reads `jobs.json`, which notebook `09_Export_Website_JSON` rewrites after every daily run,
so the website updates without any redeploy.
