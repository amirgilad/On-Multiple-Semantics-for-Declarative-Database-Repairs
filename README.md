# Semantics for Data Repair

The repository is the implementation of the approach detailed in the paper [On Multiple Semantics for Declarative Database Repairs](https://dl.acm.org/doi/10.1145/3318464.3389721) published at SIGMOD 2020. 

We study the problem of database repairs through a rulebased
framework that we refer to as Delta Rules. Delta rules
are highly expressive and allow specifying complex, crossrelations
repair logic associated with Denial Constraints,
Causal Rules, and allowing to capture Database Triggers of
interest. We show that there are no one-size-fits-all semantics
for repairs in this inclusive setting, and we consequently
introduce multiple alternative semantics, presenting the case
for using each of them. We then study the relationships
between the semantics in terms of their output and the complexity
of computation. Our results formally establish the
tradeoff between the permissiveness of the semantics and its
computational complexity. 


## Requirements
1. Python 3
1. Python packages in the requirements.txt file 
2. PostgerSQL (latest version should work)   

## Usage
1. Create an empty database called "cr" in PostgerSQL manually
3. Change the password in the dba.py file to your PostgerSQL password
2. See running_example folder

## License
[MIT](https://choosealicense.com/licenses/mit/)
