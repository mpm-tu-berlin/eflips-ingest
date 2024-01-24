# How was the `.xsd` generated?

1. Take a set of BVG XML files
2. Remove the `ns2:` and `:ns2` strings, which would break the `xsdata` package later on. 
    1. `for file in $(ls .); do sed -i -e 's/ns2://g' $file; done`
    2. `for file in $(ls .); do sed -i -e 's/:ns2//g' $file; done`
3. Install [XMLSpy](https://www.altova.com/de/xmlspy-xml-editor) on a windows machine.
4. Follow the instructions [here](https://www.altova.com/blog/generating-a-schema-from-multiple-xml-instances/) in order to generate an XSD file. **Be sure to limit the "create enumerations" option at the bottom of the "Generate DTD/Schema" dialog. Otherwiese you will get a 20MB DTD containing all possible values as Enums**.
5. Edit the file to taste. Remove any pointsless enumerations that snuck through.