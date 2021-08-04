# Care Management (CM) Patient Record Export

### Description

This is a partner-agnostic reporting tool that compiles and outputs member-specific Commons data. 
Run this script and share the output with stakeholders upon request.

### PDF vs Excel

Run the argparse arguments `--pdf` or `--excel` to run the respective code for your desired output format. 
You may also output both formats for a given run by including both arguments in your command.

**NOTE**: Until we arrive upon a stable, agreed upon version of this report with our partners, we will retain both the excel version and pdf version
in this script.

### Number of Members Per Run

`--memberIds` will take from 1 to n number of member IDs 
depending on how many you would like to run at a time.
You must input at least 1 member ID. 
Write this portion in your command as follows:
```bash
--memberIds memberId1 memberId2 memberId3 ...
```

### Dependencies

- Before running, install package dependencies in the `requirements.txt`
- Also, separately install `wkhtmltopdf`, which is an underlying system dependency for pdfkit, by running: 
```bash
brew install Caskroom/cask/wkhtmltopdf
```
Upon installing, `wkhtmltopdf` should show up in your `'/usr/local/bin/'` by default, 
but if you run into any trouble related to this dependency, 
double check that the executable for `wkhtmltopdf` can be found at `'/usr/local/bin/wkhtmltopdf'` 
and if not, move it to that location.
