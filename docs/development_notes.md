# Development Notes #

Since pyyaml is a pip-installed package with C-extensions but without an available wheel, I had to package it and include it in the /vendor directory. See [here](http://chalice.readthedocs.io/en/latest/topics/packaging.html) for more information. Note that you MUST use an EC2 using the Amazon linux to create the wheel.

# Running tests locally
We have unittest which can be run with the following code on terminal  
`python -m test <ClassName>.<test_name>`  
i.e.  
`python -m test TestCheckUtils.test_get_check_strings`
