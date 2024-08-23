{% docs __basetransactions_for_contest %}

"2024-08-22"

This view makes some changes to the transactions data which are specific to contest reporting. 
The logic is recreated from the existing process running on SQL server.
There is not supposed to be any changes made to it, so I didn't.

1) The RepID is added to the data. 
 This is the insurance agent code to link the data to the life codes. 
 This field comes from the representatives_vc table, it is the INSAGENTCODE field. 
 Do not get confused by the REPID field in the representatives_vc table.  It is not used.

 2) The transactions which belong to a joint code which is in the table jointrepresentatives_vc have been split between the 2 individual codes.  
 The amounts have been split as well.  The transaction records are adjusted. 
 The UNION query inside this view is the UNION of the joint code records adjusted and the unchanged single code records.

{% enddocs %}

{% docs __base_segfund_transtypes %}

"2024-08-23"

This view contains the hardcoded values of the transaction type codes which are used to identify which transactions to use for SegFund credits.
These are the same codes which were in the SQL server version of contest reporting.  If there needs to be transactions types added or removed this is the view to edit.
There should not need to be other changes.

{% enddocs %}