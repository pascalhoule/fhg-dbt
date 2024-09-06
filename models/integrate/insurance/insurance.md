{% docs broker_fh %}

This table contains the information about the brokers, their e-mail addresses and segment, map segment and circle of support information. 
Information from several iPipeline tables is combined in order to make the segment and circle of support easier to use in reporting. 
The logic was originally in the segmentation report and built in power bi (Pascal Houle).  In Q1 and Q2 2024 this has been written in Snowflake (Evelyn Wynen).  
The table has been expanded to include all circle of support roles as at Aug 31.

The segment tags in brokertags_vc are separated from one column called TAGNAME into a separate column for each tag.
The circle of support columns come from the ROLE field in brokercos_vc they are transformed into a separate column for each role.

This view will need to be maintained to add or remove tags and to add or remove roles.  Changes won't automatically flow through this view.

{% enddocs %}