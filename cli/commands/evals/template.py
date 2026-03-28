PROMPT_TEMPLATE = """Is there any scientific or medical evidence to support an association between the {seed_type} {seed_name} and the {target_type} {target_name}? Please rate the strength of the evidence on a 5-point scale, where:
    1 = No evidence (zero papers mentioning both {seed_name} and {target_name})
    2 = Weak evidence (1-2 papers mentioning both {seed_name} and {target_name} and no experimental evidence)
    3 = Moderate evidence (3-4 papers mentioning both {seed_name} and {target_name} or experimental evidence)
    4 = Strong evidence (5-6 papers mentioning both {seed_name} and {target_name} or several experimental studies)
    5 = Very strong evidence (more than 6 papers mentioning both {seed_name} and {target_name} or substantial experimental evidence)
    In your response, please also explain the reasoning behind your rating and reference any relevant scientific or medical sources (e.g., peer-reviewed studies, clinical guidelines, experimental data) that support your assessment. For each part of your response, indicate which sources most support it via citation keys at the end of sentences, like (Example2012Example pages 3-4). Only use valid citation keys.

    Instructions to the LLM: Respond with the following XML format exactly.
    <response>
    <reasoning>...</reasoning>
    <rating>...</rating>
    </response>

    `rating` is one of the following (must match exactly): 1, 2, 3, 4, or 5. Do not include any additional keys or text."""
