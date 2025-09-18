import logging


class CreateIndicesStep:
    def __init__(self, factory):
        self.factory = factory

    def execute(self):
        db = self.factory.create_connection()

        self.add_indices(db)
        self.add_graphs(db)

    @staticmethod
    def add_indices(db):
        # indexes and constraints will be created in later steps but add them here for now
        db.collection('assets').add_persistent_index(fields=['assettype_key'], unique=False, sparse=False)
        db.collection('assets').add_persistent_index(fields=['toezichter_key'], unique=False, sparse=False)
        db.collection('assets').add_persistent_index(fields=['toezichtgroep_key'], unique=False, sparse=False)
        db.collection('assets').add_persistent_index(fields=['beheerder_key'], unique=False, sparse=False)

        db.collection('assetrelaties').add_persistent_index(fields=["relatietype_key"], unique=False, sparse=False)

        db.collection('assettypes').add_persistent_index(fields=['short_uri'], unique=False, sparse=False)
        db.collection('relatietypes').add_persistent_index(fields=['short'], unique=False, sparse=False)
        db.collection('betrokkenerelaties').add_persistent_index(fields=['_from', 'role'], unique=False, sparse=False)
        db.collection('betrokkenerelaties').add_persistent_index(fields=['_to', 'role'], unique=False, sparse=False)
        db.collection('vplankoppelingen').add_persistent_index(fields=['assets_key'], unique=False, sparse=False)

    @staticmethod
    def add_graphs(db):
        if db.has_graph("assetrelaties_graph"):
            db.delete_graph("assetrelaties_graph", drop_collections=False)
        assetrelaties_graph = db.create_graph("assetrelaties_graph")
        assetrelaties_graph.create_edge_definition(
            edge_collection="assetrelaties",
            from_vertex_collections=["assets"],
            to_vertex_collections=["assets"]
        )

        if db.has_graph("betrokkenerelaties_graph"):
            db.delete_graph("betrokkenerelaties_graph", drop_collections=False)
        betrokkenerelaties_graph = db.create_graph("betrokkenerelaties_graph")
        betrokkenerelaties_graph.create_edge_definition(
            edge_collection="betrokkenerelaties",
            from_vertex_collections=["assets", "agents"],
            to_vertex_collections=["agents"]
        )

        if db.has_graph("bestekkoppelingen_graph"):
            db.delete_graph("bestekkoppelingen_graph", drop_collections=False)
        bestekkoppelingen_graph = db.create_graph("bestekkoppelingen_graph")
        bestekkoppelingen_graph.create_edge_definition(
            edge_collection="bestekkoppelingen",
            from_vertex_collections=["assets"],
            to_vertex_collections=["bestekken"]
        )

        if db.has_graph("aansluitingen_graph"):
            db.delete_graph("aansluitingen_graph", drop_collections=False)
        aansluitingen_graph = db.create_graph("aansluitingen_graph")
        aansluitingen_graph.create_edge_definition(
            edge_collection="aansluitingen",
            from_vertex_collections=["assets"],
            to_vertex_collections=["aansluitingrefs"]
        )






