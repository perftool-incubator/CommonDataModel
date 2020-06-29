In this directory, you can:

1) Build the index templates for the indices we have
2) Generate commands to load the index templates
3) Generate the commands to create indices
4) Generate the commands to remove everything from ES

To build the index templates, run 'make'.

If you are interested in changing the mappings, edit the
base files ($index.base), run 'make clean' and then 'make'.

Many of the indices have some fields in common, and so the
Makefile uses 'build.sh' to combine certain .base files to
form many of the .json files.

To generate any of the commands, you must have a URL where 
your ES instance is available.

If you are creating a new ES for CDM, you would first need to
build the templates, then run 'create-indices-cmds.sh' to generate
the commands to create the indices, then run those commands.

After index templates are loaded, indices can be created with
'create-indices-cmds.sh', which will provide the commands
to run to generate the indices.

If you wish to remove all indices and templates, running
'create-cleanup-cmds.sh' will provide all of the commands
to remove everything.
