# NYC Property Record Extractor

This is the tool used for downloading ACRIS data and outputing human readable table for further analysis.
Trying to answer the simple question: who at what time bought what type of property at what location with how much money.
Output data including:

1. document id
2. document date
3. document amount
4. location: borough, block, lot, unit, street number, street name
5. buyers name

## Process

1. Retrive master record as year input
2. Retrive legal and parties record by the first four digits of document_id matches the year input
3. For every master record that has different first four digits of document_id and the year, retrive legal and parties record
4. Merge all record on document_id
