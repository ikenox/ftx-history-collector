# ftx-history-downloader

A script to download FTX execution history of a specific FTX account.

## Disclaimer

This script is not tested well. Please use at your own risk.

## Usage

```shell
# Before doing, please make a credential json file like the following format
# It's recommended to use an API key which has only read permission
$ cat ./credential.json
> {"api_key": "XXX", "api_secret": "XXX"}

# Execute
$ cargo run --release -- \
    # Specify a directory in which you want to save downloaded data
    --outdir ./output \
    --credential ./credential.json \
    # You can omit this option if you want to obtain main account's history
    --sub-account sub1
    
# The collected history will be saved to the specified output directory
$ ls ./output
> sub1-2020-11-21.tsv sub1-2020-12-10.tsv sub1-2021-10-11.tsv sub1-2021-11-21.tsv
  sub1-2020-11-22.tsv sub1-2020-12-20.tsv sub1-2021-10-13.tsv sub1-2021-11-24.tsv
  sub1-2020-11-29.tsv sub1-2021-04-03.tsv sub1-2021-11-10.tsv sub1-2021-11-25.tsv 
  ...
```

## License

MIT License

Copyright (c) 2022 Naoto Ikeno

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.