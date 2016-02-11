Most services in the FreeNAS 10 middleware API have a `query` method. They all take the same args:

`service.query( array filter, object params )`

Filter is an array that represents the filter to be applied to the array. Filter arguments should be a 2-, 3- or 4-tuples in one of three supported formats:

    - 2-tuple - carries logic operators. it's composed of operator name and list of sub-criterions to be concatenated using it in following format: ``[<operator name>, [<tuple1>, <tuple2>, ...]]``. Operator name can be one of: ``and``, ``or`` or ``nor``. Example: ``["or", ["username", "=", "root"], ["full_name", "=", "root"]]``

    - 3-tuple - carries single criterion composed of field name, operator and compared value in format ``[<field name>, <operator>, <value>]``. Example: ``["username", "=", "root"]``

    - 4-tuple - same as above, but has also notion of "conversion operator". It's stored as a fourth tuple element. Conversion operator is right now used only to convert ISO8601-compatible time (expressed as string) to UNIX timestamp on server side.

    Following operators are supported:

    -  ``=``
    -  ``!=``
    -  ``>``
    -  ``<``
    -  ``>=``
    -  ``<=``
    -  ``in`` - value in set
    -  ``nin`` - value not in set
    -  ``~`` - regex match

    Following conversion operators are supported:

    - ``timestamp`` - converts ISO8601-compatible timestamp expressed as string to UNIX timestamp

`params` is an object containing additional mutations to the output of the query. The supported properties are:

    -  ``offset`` - skips first ``n`` found objects
    -  ``limit`` - limits query to ``n`` objects
    -  ``count`` - returns matched objects count
    -  ``sort`` - name of field used to sort results. It may be ``field`` or ``-field`` to sort in ascending or descending order respectively. Arrays of fields are also accepted, eg. ``["-field1", "field2", "-field3" ]``
    -  ``single`` - returns single object instead of array. If multiple objects were matched in the query, returns first one (in random order if ``sort`` was not specified).
