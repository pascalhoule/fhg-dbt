{% docs __COMM_WGTS_insurance %}

The purpose of this view is to determine for each policycode and ownercode (commissioning agent) what the commission split is.
Within the accurals file the commissioning agent percentages do not cleanly work out to 100% for each policycode or policy.
Some of the reasons for this include:
1) Chargebacks or other one-off adjustments.  These are often entered with a split of 1 (100%) even if the commission on the policy is to be split.
2) A commission which is withheld from being paid.  In these cases the total split on the policy is less than 1.

This is done separately because it is quite involved and needs to be tested separately.

{% enddocs %}