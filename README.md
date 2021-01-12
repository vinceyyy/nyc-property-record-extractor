# NYC Property Record Extractor

This is the tool used for downloading ACRIS data and outputing human readable individual-bought record table for further analysis.
Trying to answer the simple question: who at what time bought what type of property at what location with how much money.
Output data including:

1. document id
2. document date
3. document amount
4. location: borough, block, lot, unit, street number, street name
5. buyers name
6. nationality: chinese buyers (1) or non-chinese buyers (2)

To understand the output you also need:

1.

## Process

1. Retrive master record (only DEED and RPTT&RET) according to the year input
2. Retrive legal and parties record by the first four digits of document_id matches the year input
3. For every master record that has different first four digits of document_id and the year, retrive legal and parties record (and keep only the buyers' record)
4. Merge all record on document_id and clean the data
5. Mark all chinese-bought properties, using the surname list from [chinese-surname-spellings](https://github.com/vinceyyy/chinese-surname-spellings)
