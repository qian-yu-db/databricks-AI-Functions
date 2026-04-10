# Use Case Description

The insurance call center operation is an important customer facing function for any insurance company. The call center is responsible for handling customer inquiries, processing claims, and providing support to policyholders. The operation of the call center can have a significant impact on customer satisfaction and retention, as well as the overall efficiency of the insurance company.

The goals of the analysis are to: 

1. study and track the performance of operators and satisfaction of the customers to identify the area of improvement
2. provide personalized recommendations to the operators to improve the customer experience

In this example, we will demonstrate how to use the AI function for Databricks LLM Batch 
Inference to analyze customer call transcripts and provide insights into customer intent, 
sentiment, compliance with company policies. We will also show how to recommend the next 
best action for the customer service agent based on the analysis.

## Data Sources

* Customer Call Transcripts
* Customer Profile
* Company Policy Information
* Compliance Information

## AI Functions Used

* `ai_analysis_setiment()`
* `ai_summarize()`
* `ai_query()`

## Prompts Script Example:

* Customer Intent Extraction

    ```
    You are a customer service agent for an insurance company. Your task is to extract the customer intent from the call transcript.
    Given the following transcript, {transcript} extract the customer intent of either 'auto accident', 'home accident', 'motocycle', 
    or 'general policy') and key context of the intent
    ```
  
* Compliance Analysis

    ```
    You are a compliance officer for an insurance company. Your task is to analyze the call transcript for compliance issues.
    Given the following transcript, {transcript} identify violations of the operation guidelines: {guilde lines}
  
    each violation will deduct 1 point from the total score of 10. return the score and the list of violations
    ```
  
* Next best action recommendation

    ```
    You are a customer service agent for an insurance company. Your task is to recommend the next best action for the customer service agent.
    Given the following transcript, {transcript} and the customer profile, {customer_profile} recommend one of the next best actions: 
    'follow-up call', 'promotional email', 'apology email', 'no action' for the customer service agent.
    ```

## AI Functions Example

```sql
SELECT
    transcript,
    ai_analyze_sentiment(
        transcript
    ) AS sentiment,
    ai_summarize(
       transcript,
       150
    ) AS summary,
    ai_query(
      "databricks-meta-llama-3-3-70b-instruct",
      concat("You are a compliance officer for an insurance company. Your task is to analyze the call transcript for compliance issues.", 
      "Given the following transcript: ", transcript, "extract the customer intent of either 'auto accident', 'home accident', 'motocycle', 'general policy'",
      "and key context of the intent"),
      responseFormat => 'STRUCT<intent_analysis:STRUCT<intent:STRING, context:STRING>>',
      failOnError => false
    ) as intent_analysis,
  ai_query(
    "databricks-claude-3-7-sonnet",
    concat("You are a compliance officer for an insurance company.",
           "Using the following 5 guidelines:", (select guidelines from fins_genai.call_center.call_center_guidelines),
           "to identify the violations from the customer service operators based on the following transrcipt:",
           "<transcript>", transcript, "</transcript>\n",
           "each violation on a guideline count as 1 violation and max number of violations is 5"
           "each operator starts with 10 points, deduct 1 point for each violations"
           "return the final points in a json {'points': INT'}, do not explain"
           ""),
    returnType => 'STRUCT<points:INT>'
    ) as compliance_score,
  ai_query(
    "databricks-claude-3-7-sonnet",
    concat("You are a compliance officer for an insurance company.",
           "Using the following guildlines:", (select guidelines from fins_genai.call_center.call_center_guidelines),
           "to identify the violations from the customer service operators based on the following transrcipt:",
           "<transcript>", transcript, "</transcript>\n", 
           "each violation on a guideline count as 1 violation and max number of violations is 5"
           "Give justifaction to each violation")
    ) as compliance_violations_justification,
  ai_query(
      "databricks-meta-llama-3-3-70b-instruct",
      concat("You are a customer service agent for an insurance company.", 
             "Your task is to recommend the next best action for the customer service agent.",
             "analysis the following transcript: ", "<transcript>", transcript, "</transcript>\n",
             "give a recommendation of the next best action from the following list of choices: ", 
             "'follow-up call', 'promotional email', 'automated email', 'apology email'",
             "Answer with one recommended action, do not explain. If no action is required, return 'none'."
             "Answer with a succinct justificaction of the recommendation in less than 5 words "),
      responseFormat => 'STRUCT<recommendation:STRUCT<catagory:STRING, justification:STRING>>'
    ) as next_best_action,
FROM customer_call_transcripts
```
