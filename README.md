# labelview

this is a small cli application that reads labels from bsky labeler services.
services can be looked up by their did, by their handle, or by going directly to
the domain that is serving the label stream.

currently the application reads all labels every time, and will summarize the
counts of currently-effective labels applied by the labeler at the end. the
labels can optionally be written to a sqlite database as well.

## example usage

```
$ labelview lookup asukafield.xyz
looking up did...
looking up did via dns TXT...
reading did document from plc directory...

handle: asukafield.xyz
did:    did:plc:4ugewi6aca52a62u62jccbl7

pds:     https://maitake.us-west.host.bsky.network
labeler: https://ozone.asukafield.xyz

streaming from labeler service
label subscription stream slowed and crawled; terminating

--------------------
--> UPDATE SUMMARY
--------------------

received a total of 20851 label record(s)

== --> last label update received was at "2024-12-14T09:08:29.870Z", which is 13m 23s 153ms 445us 357ns ago
OK --> got label records from exactly 1 labeler did (this is good)
(info) --> all source dids:
   did:plc:4ugewi6aca52a62u62jccbl7

--------------------
labeler defined 18872 effective label(s)
--------------------
did:plc:4ugewi6aca52a62u62jccbl7 labels       60 x: "!hide" (global) -> Record { kind: "app.bsky.feed.post" }
did:plc:4ugewi6aca52a62u62jccbl7 labels     3142 x: "!warn" (global) -> Account
did:plc:4ugewi6aca52a62u62jccbl7 labels        3 x: "!warn" (global) -> Record { kind: "app.bsky.feed.post" }
did:plc:4ugewi6aca52a62u62jccbl7 labels        1 x: "Transphobia" -> Account
did:plc:4ugewi6aca52a62u62jccbl7 labels        1 x: "Transphobia" -> Record { kind: "app.bsky.feed.post" }
did:plc:4ugewi6aca52a62u62jccbl7 labels      744 x: "enbyphobia" -> Account
did:plc:4ugewi6aca52a62u62jccbl7 labels        1 x: "enbyphobia" -> Record { kind: "app.bsky.actor.profile" }
did:plc:4ugewi6aca52a62u62jccbl7 labels      193 x: "enbyphobia" -> Record { kind: "app.bsky.feed.post" }
did:plc:4ugewi6aca52a62u62jccbl7 labels        1 x: "enbyphobia" -> Record { kind: "app.bsky.graph.list" }
did:plc:4ugewi6aca52a62u62jccbl7 labels        1 x: "misgendering" -> Account
did:plc:4ugewi6aca52a62u62jccbl7 labels      229 x: "misgendering" -> Record { kind: "app.bsky.feed.post" }
did:plc:4ugewi6aca52a62u62jccbl7 labels     2063 x: "transmisogyny" -> Account
did:plc:4ugewi6aca52a62u62jccbl7 labels        2 x: "transmisogyny" -> Record { kind: "app.bsky.actor.profile" }
did:plc:4ugewi6aca52a62u62jccbl7 labels     1116 x: "transmisogyny" -> Record { kind: "app.bsky.feed.post" }
did:plc:4ugewi6aca52a62u62jccbl7 labels       31 x: "transmisogyny" -> Record { kind: "app.bsky.graph.list" }
did:plc:4ugewi6aca52a62u62jccbl7 labels        2 x: "transmisogyny" -> Record { kind: "app.bsky.graph.starterpack" }
did:plc:4ugewi6aca52a62u62jccbl7 labels     9375 x: "transphobia" -> Account
did:plc:4ugewi6aca52a62u62jccbl7 labels        5 x: "transphobia" -> Record { kind: "app.bsky.actor.profile" }
did:plc:4ugewi6aca52a62u62jccbl7 labels     1877 x: "transphobia" -> Record { kind: "app.bsky.feed.post" }
did:plc:4ugewi6aca52a62u62jccbl7 labels       24 x: "transphobia" -> Record { kind: "app.bsky.graph.list" }
did:plc:4ugewi6aca52a62u62jccbl7 labels        1 x: "transphobia" -> Record { kind: "app.bsky.graph.starterpack" }
```

## the reason for the tool

bluesky moderation is, to put it mildly, a mess. composable moderation is a bad
idea for online communities, and the implementation of labelers was an
afterthought:

* because of a [major and long-unaddressed bug][hidebug], any labeler -- even
  "fun" labelers -- can apply any global moderation labels including `!warn` and
  `!hide`
* it's difficult to tell that a `!hide` label has even been served to you,
  because the post or user is usually just removed
* if you happen to see it anyway, it is possible to see which labeler applied
  the label via clicking the "Learn more" link, but this is not obvious
* because this is not configurable, there is no way to get around the block
  without unsubscribing from the labeler entirely. **this makes labelers
  categorically more powerful and less accountable than blocklists, which are
  already a terrible idea.**

[hidebug]: https://github.com/bluesky-social/atproto/issues/2367

## caveats of this tool

this tool is as-is. it may accept some data that's technically not 100%
canonical for labelers. but if the state of things is any indication, it's
probably stricter than most of the other systems that look at them.

it makes no attempt to look at or validate the cryptographic signatures of the
label records, though it does save them to sqlite when you use that mode. (the
signatures are supposed to be signed via the labeler's `#atproto_label` key
found under `verificationMethod`; see the [did standard][didstd].)

[didstd]: https://www.w3.org/TR/did-core/#dfn-publickeymultibase

you may notice it's also not licensed. you can use it. if you know how you
should clone it, use [`rustup`][rustup] and `cargo` to build and run it, modify
it, etc.; if you don't know how to do that, i built some versions of the binary
for common operating systems in the [releases][releases] section, probably. you
shouldn't have to trust me and just download random stuff like that, but if you
do i promise i didn't do anything weird.

[rustup]: https://rustup.rs/

[releases]: https://github.com/widrs/labelview/releases

## sqlite output

to get a [sqlite][sqlite] dump of the labels, simply provide the `--save-to-db`
flag with a file name or path and the sqlite file at that path will be created
or reused. a table named `label_records` will be created with the values of the
labelrecords and the timestamp that the records were received from the service.
if there are already label records in the table from another export, more
exports will just add more labels.

[sqlite]: https://sqlite.org/

to read from the sqlite database, if you are not familiar, i recommend checking
out [sqlite studio][studio] or the "sqlite shell" available for the command line
as part of the set of tools that comes with the precompiled binaries download
for sqlite.

[studio]: https://sqlitestudio.pl/

determining what labels are currently effective for a set of label records can
be complex. the logic appears to be as follows (all queries below assume that
only one export is present in the `label_records` table):

```sql
with effective as (
    select
        *,
        row_number() over (
            partition by src, target_uri, val
            order by seq desc
        ) as recency
    from label_records
)
select
    src as labeler_did,
    val as applied_label,
    target_uri as target_did_or_record,
    target_cid as target_record_version
from effective
where
    recency = 1 and
    not neg and
    (
        expiry_timestamp is null or
        unixepoch(expiry_timestamp) > unixepoch(current_timestamp)
    )
```

## the problems with the data model

* labelers do not commit to the full set of labels they declare. only the
  individual records they declare are actually signed, and they are designed to
  disappear when they are superceded.
* labels are only provided by the service run by the entity operating the
  labeler, who is free to do whatever weird thing they can dream up with the
  labels, including emitting different labels all the time, showing different
  labels to different requesters, changing labels retroactively, etc.
* because labels are designed to be synchronized using a permanent sequence
  number that is implicitly trusted, frontend services like the bsky frontend
  may never actually go back and reingest these old retconned labels.
* because the only real way to enumerate all labels is to ask the labeling
  entity, this means there is no way to prove that labels *aren't* applied.
  * the only real way to tell whether your favorite bsky frontend that applies
    labels that you don't own and operate is to see those labels get applied.

while working on the logic to persist previously seen labels and audit any seen
changes from a labeler over multiple passes for suspicious changes (which is,
frankly, just a huge pain in the ass considering how messy the data can get) the
situation with bsky """trust & safety""" has rapidly deteriorated, and i have
opted to instead release this tool as-is with simpler functionality. it still
outputs the labels it reads, which you can analyze yourself if you desire.

there are even more problems than that:

* the labeling is somewhat underspecified.
  * what actually happens when a positive label supercedes another positive
    label with a different version is not well-defined
  * what happens when a negation label has a different, shorter expiry than the
    label it is negating is not defined
  * what should be done when most label records' signatures validate, but
    certain records' signatures do not, is not defined. this is not helped by
    the fact that key rotation has a built in expectation that there may be a
    period of time where most or all the labels will appear to be improperly
    signed by the service

80% of the real bad parts of this specific corner of the system would be
mitigated if they would just fix the problem where labelers can always apply
undeclared global moderation tags and block anyone and all accounts they feel
like. it would not solve the rot at the heart of the place, but it would help a
lot.
