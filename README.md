# labelview

this is a small cli application that reads labels from bsky labeler services.
services can be searched by their did, by their handle, or by going directly to
the service that is serving the label stream.

currently the application reads all labels every time, and will summarize the
counts of currently-effective labels applied by the labeler at the end. the
labels can optionally be written to a sqlite database as well

## the reason for the tool

TODO(widders): this

## sqlite output

to get a sqlite dump of the labels, simply provide the `--save-to-db` flag with
a file name or path and the sqlite file at that path will be created or reused.
a table named `label_records` will be created with the values of the label
records and the timestamp that the records were received from the service. if
there are already label records in the table from another export, more exports
will just add more labels.

determining what labels are currently effective for a set of label records can
be complex. the logic appears to be as follows:

```
with effective as (
    select
        *,
        row_number() over (
            partition by src, target_uri, val
            order by seq desc
        ) as recency
    from label_records
)
select src, val, target_uri, target_cid
from effective
where
    -- TODO(widders): exp too
```

## the problems with the data model
