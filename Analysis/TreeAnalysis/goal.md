The goal is to create a script that finds the different tree structures within a database, lists them and then assign them given one asset in that tree

This script will live inside /Analysis/TreeAnalysis
It will use the arangoDb that is being created with the scripts in this repo.
Use the credentials from the settings file in main_linux_arango.py to connect to the database.

First, we need to make a list of the different tree structures.
Only asset with a naampad should be considered for this, as assets without naampad are not part of a tree structure.
For performance: the naampad also exsists in the attribute naampad_parts as a array.
Assets are in the same tree when the first element in naampad_parts is identical.

For tree structures: 
A tree structure should take into account assettypes (short_uri) but not the amount of assets with the same type on the same level.
For example a structure with, root, 1 WV and 100 VPLMast is the same structure as root, 2 WV with 50 VPLMast each as both can be reduced to 
(root)
  (WV)
    (VPLMAST)

I want this script to first find all different trees within the database. These should be dumped in a json file, with the first occurence of that tree.
Then I want to be able to label each tree
As the final step I want to have the uuids of LSDeel assets within that tree added, so I can easily match those later.

