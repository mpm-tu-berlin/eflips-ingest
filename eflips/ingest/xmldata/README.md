# How was this generated?

1. Create a `.xsd` file (see `data/README.md`) for details.
2. Edit the file to make sense, remove `xs:restriction` blocks that are pointless
3. Run `xsdata data/bvg_xml.xsd  --kw-only --include-header --subscriptable-types  -p eflips.ingest.xmldata` in the project root to generate the code
4. Remove the extraneous `eflips/__init__.py` that is negerated.