# Use Case Description

Feature engineering is the process of transforming raw data into a numerical format that can be 
used by machine learning algorithms. Text data often contains a lot of information that can be 
very valuable as features for machine learning models. However, they require cleaning 
and transformation to be used as features. With Databricks LLM Batch Inference and AI Functions, 
we can make this process much easier and more efficient.

In this example, we will demonstrate how to use the AI function for Databricks LLM Batch to 
generate a categorical features from a text data column as part of the feature engineering 
process. the use insurance claim description as an example.

## Data Sources

* Raw Feature Data with Text (string) data columns and unique identifier
* Numerical features data with unique identifier


## AI Functions Used

* `ai_classify()`
* `ai_query()`

## Prompts Script Example:

```
You are an expert claims adjustor for an insurance company. 
Your job is to read the following claim description and determine its complexity (high, medium, low). 
When performing your task, keep the following things in mind:

    * Any personal injury claims that involve a head injury are automically considered complexity
    * Any claims that involve a head injury are automically considered high complexity
    * Any claims with flooding or water injury are automically considered high complexity.

claim description: {description}
```

## AI Functions Example

```sql
SELECT 
   claim_id,
   concat('You are an expert claims adjustor for an insurance company.
          Your job is to read the following claim description and determine its complexity
          (high, medium, low). When performing your task, keep the following things in mind:

        * Any personal injury claims that involve a head injury are automically considered complexity
        * Any claims that involve a head injury are automically considered high complexity
        * Any claims with flooding or water injury are automically considered high complexity.
        
        CLAIM DESCRIPTION:', claim_description) as claim_description_with_prompt,
   ai_classify(claim_description_with_prompt, array("high", "medium", "low")) as ai_complexity_rating
FROM claims_classification_text_feature
```

## Notebook Example