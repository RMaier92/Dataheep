# Dataheep
Storing all level of data maturity in one heep. Having raw, filtered aggregated and even predicted data of one source there.

## For whom to use?
Data scientists and analysts, cleaning and preparing their data for further predictions.

## Why to use data heep?
### Problem: 

   ... raw data needs to be cleaned ...

   ... cleaned data needs to be filtered ...

   ... filtered data needs to be aggregated ...

   !! By doing this procedure for one dataset, there are already three variables used then !!


   ### Example:
   - Original dataset: oil_prices.csv

     -> Variable for cleaned dataset: df_oil_prices_cleaned

     -> Variable for filtered dataset: df_oil_prices_filtered

     -> Variable for aggregated dataset: df_oil_prices_aggregated
     
     Now there are three variables from the same original dataset, but with different maturity level. How do i know, that these variables belong togehter?

     Use Dataheep :-)

    
    
    

## How to use (full example is under sample.ipynb (jupyter notebook))
```python
import pandas as pd
from dataheep import Dataheep

df_oil_original = pd.read_csv("oil.csv")

"""
First of all, a new dataheep needs to be created. 
This dataheep doens't contain any attributes
"""
dhp_oil = Dataheep.object_create()

"""
Since the dataheep is empty and doesn't know any attribute,
the required attributes (here data maturity levels) are added
"""
dhp_oil = Dataheep.attribute_add(dhp_oil, "original_data")
dhp_oil = Dataheep.attribute_add(dhp_oil, "cleaned_data")
dhp_oil = Dataheep.attribute_add(dhp_oil, "aggregated_data")
dhp_oil = Dataheep.attribute_add(dhp_oil, "predicted_data")


""" 
Now this attributes can be used to assign a value.
"""
dhp_oil.original_data = df_oil_original

"""
Doing some data modification and adding the new data also to dhp_oil 
"""
dhp_oil.cleaned_data = clean_oil_data( dhp_oil.original_data )
dhp_oil.aggregated_data = aggregate_oil_data( dhp_oil.cleaned_data )
dhp_oil.predicted_data = predict_oil_data( dhp_oil.aggregated_data )

"""
Since all preparation step take some time, the dataheep can be saved

"""

Dataheep.object_save(dhp_oil, "./oil.dhp")



