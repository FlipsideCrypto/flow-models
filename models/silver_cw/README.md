# SILVER_CW / The Streamline Migration

This directory is a copy of the silver directory with models downstream of the Chainwalkers source. Chainwalkers is being de-commissioned in October 2023, but the Streamline backfill will take some time to finish. These models are thus copied over as an archive of the Chainwalkers data, and to keep the new (streamline) models separate and organized.  
  
This entire directory may be deleted once Chainwalkers is fully deprecated.  
  
New models that have been migrated to use Streamline as a source are suffixed with `_s`. No suffix was added for models that do not require a migration (prices, labels, external APIs, etc.). While I would prefer the new models have no suffix, the decision was made to append `_s` to the new streamline models (instead of copying data into new tables and appending `_cw` to the deprecating data) to minimize the touchpoints on prod data. A decision may be made once the CW data is fully deprecated to drop the suffix from all models and rebuild in cleanly named tables.  

The `scheduled` tag will remain on all models that should run on an hourly schedule. 2 new tags have been added for org purposes: `streamline_scheduled` and `chainwalkers_scheduled` to enable clear model selection. These tags were added only where a model was already tagged with `scheduled` to limit net-new.  
