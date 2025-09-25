import logging


class CreateIndicesStep:
    def __init__(self, factory):
        self.factory = factory

    def execute(self):
        db = self.factory.create_connection()

        self.add_indices(db)
        self.add_graphs(db)
        self.ensure_naampad_analyzer_and_view(db)

    @staticmethod
    def add_indices(db):
        # indexes and constraints will be created in later steps but add them here for now
        db.collection('assets').add_persistent_index(fields=['assettype_key'], unique=False, sparse=False)
        db.collection('assets').add_persistent_index(fields=['toezichter_key'], unique=False, sparse=False)
        db.collection('assets').add_persistent_index(fields=['toezichtgroep_key'], unique=False, sparse=False)
        db.collection('assets').add_persistent_index(fields=['beheerder_key'], unique=False, sparse=False)
        db.collection('assets').add_persistent_index(fields=['naampad_parts'], unique=False, sparse=True)
        db.collection('assets').add_persistent_index(fields=['assettype_key', 'AIMDBStatus_isActief'], unique=False, sparse=False)
        db.collection('assets').add_persistent_index(fields=['assettype_key', 'AIMDBStatus_isActief', 'toestand'], unique=False, sparse=False)

        db.collection('assetrelaties').add_persistent_index(fields=["relatietype_key"], unique=False, sparse=False)
        db.collection('assetrelaties').add_persistent_index(fields=['relatietype_key', 'AIMDBStatus_isActief'], unique=False, sparse=False)

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
        
    def ensure_naampad_analyzer_and_view(self, db):
        """
        Ensure the edge_ngram analyzer and ArangoSearch view for assets.naampad_parts.
        This is specific for the use case: naampad_parts in the assets collection.
        Only uses the python-arango API.
        """
        return

        # analyzer is still an experimental feature in arangodb 3.12, so we skip this for now

        analyzer_name = "naampad_edge_ngram"
        view_name = "naampadView"
        collection = "assets"
        # Ensure analyzer using python-arango API
        analyzers = getattr(db, "analyzers", None)
        create_analyzer = getattr(db, "create_analyzer", None)
        if not (callable(analyzers) and callable(create_analyzer)):
            raise RuntimeError("python-arango does not support analyzer API on this db object")

        existing = {a["name"] for a in db.analyzers()}
        if analyzer_name not in existing:
            min_n = 1
            max_n = 10

            db.create_analyzer(
                name=analyzer_name,
                analyzer_type="edge_ngram",
                properties={"min": min_n, "max": max_n, "preserveOriginal": True},
                features=[],
            )
            logging.info("Created analyzer %s", analyzer_name)
        else:
            logging.info("Analyzer %s already exists", analyzer_name)

        # Ensure the ArangoSearch view exists and is linked (python-arango API)
        views_list_fn = getattr(db, "views", None)
        create_view_fn = getattr(db, "create_view", None)
        if not callable(views_list_fn) or not callable(create_view_fn):
            raise RuntimeError("python-arango does not support view API on this db object")

        existing_views = {v["name"] for v in db.views()}
        links = {
            collection: {
                "fields": {
                    "naampad_parts": {
                        "analyzers": [analyzer_name]
                    }
                },
                "includeAllFields": False,
                "storeValues": "none",
            }
        }
        if view_name not in existing_views:
            db.create_view(
                name=view_name,
                view_type="arangosearch",
                properties={"links": links}
            )
            logging.info("Created view %s with naampad_parts link", view_name)
        else:
            # Update the view's links if needed
            view = db.view(view_name)
            # This assumes python-arango supports updating view properties (if not, this will need to be extended)
            view.update(properties={"links": links})
            logging.info("Patched view %s with naampad_parts link", view_name)