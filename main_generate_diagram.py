import json
from pathlib import Path

from arango import ArangoClient


def load_settings(settings_path: Path) -> dict[str, object]:
    import json
    with open(settings_path, 'r') as file:
        return json.load(file)


def generate_model(db):
    model = {"vertices": [], "edges": []}

    # Helper: sample up to N docs from a collection
    def sample_attrs(col_name, sample_size=10):
        col = db.collection(col_name)
        attrs_set = set()
        for i, doc in enumerate(col.all()):
            if i >= sample_size:
                break
            attrs_set.update(doc.keys())
        # only keep the fields ending in _key or _uuid
        return sorted([a for a in attrs_set if a.endswith("_key") and a != "_key"])

    # 3. Populate vertices with their foreign-key attributes
    for c in db.collections():
        name = c["name"]
        if name.startswith("_") or name == 'params':
            continue
        model["vertices"].append({
            "name": name,
            "fkeys": sample_attrs(name)
        })

    # 4. Populate edges with their foreign-key attributes
    for g in db.graphs():
        graph = db.graph(g["name"])
        for ed in graph.edge_definitions():
            ec = ed["edge_collection"]
            for frm in ed["from_vertex_collections"]:
                for to in ed["to_vertex_collections"]:
                    model["edges"].append({
                        "name": ec,
                        "from": frm,
                        "to": to,
                        "fkeys": sample_attrs(ec)
                    })
    return model


def generate_drawio_json_with_fkeys(
        model,
        cols: int = 3,
        fkey_mapping: dict = None,
        vertex_width: int = 140,
        vertex_height: int = 60,
        x_margin: int = 100,
        y_margin: int = 50,
        x_spacing: int = 200,
        y_spacing: int = 150
):
    """
    Generate Draw.io JSON from a model dict,
    including both explicit graph edges and edges inferred from fkeys.

    model = {
      "vertices": [
        { "name": "assets",      "fkeys": ["assettype_key", ...] },
        { "name": "assettypes",  "fkeys": []                   },
        ...
      ],
      "edges": [
        { "name": "assetrelaties",       "from": "assets", "to": "assets",       "fkeys": ["relatietype_key"] },
        { "name": "bestekkoppelingen",   "from": "assets", "to": "bestekken",    "fkeys": [] },
        ...
      ]
    }

    fkey_mapping: optional dict of exact fkey→target overrides, e.g.
      {
        "toezichter_key": "identiteiten",
        "toezichtgroep_key": "toezichtgroepen"
      }
    """
    if fkey_mapping is None:
        fkey_mapping = {}

    # 1) Prepare a deduplicated list of all edges
    all_edges = []
    seen = set()

    # a) start with your explicit edges
    for rel in model["edges"]:
        key = (rel["from"], rel["to"], rel["name"])
        if key not in seen:
            seen.add(key)
            all_edges.append({
                "label": rel["name"],
                "from": rel["from"],
                "to": rel["to"]
            })

    # b) add edges inferred from each vertex's fkeys
    for vert in model["vertices"]:
        src = vert["name"]
        for fk in vert["fkeys"]:
            # find target collection
            if fk in fkey_mapping:
                tgt = fkey_mapping[fk]
            else:
                # strip suffix and pluralize with 's'
                base = fk.rsplit("_", 1)[0]
                tgt = base + "s"

            key = (src, tgt, fk)
            if key in seen:
                continue
            seen.add(key)
            all_edges.append({
                "label": fk,
                "from": src,
                "to": tgt
            })

    # 2) Lay out vertices on a grid
    cells = [{"id": "0"}]
    vertex_ids = {}
    next_id = 1

    for idx, vert in enumerate(model["vertices"]):
        vid = str(next_id);
        next_id += 1
        vertex_ids[vert["name"]] = vid

        row, col = divmod(idx, cols)
        x = x_margin + col * x_spacing
        y = y_margin + row * y_spacing

        # label: name + fkey attrs
        if vert["fkeys"]:
            label = vert["name"] + "\n" + "\n".join(vert["fkeys"])
        else:
            label = vert["name"]

        cells.append({
            "id": vid,
            "value": label,
            "vertex": True,
            "style": (
                "rounded=1;whiteSpace=wrap;html=1;"
                "fillColor=#dae8fc;strokeColor=#000000;"
            ),
            "geometry": {
                "x": x, "y": y,
                "width": vertex_width,
                "height": vertex_height
            }
        })

    # 3) Create edge cells
    for edge in all_edges:
        eid = str(next_id);
        next_id += 1
        src = vertex_ids.get(edge["from"])
        tgt = vertex_ids.get(edge["to"])
        if not src or not tgt:
            # skip if we don’t have that vertex in the model
            continue

        cells.append({
            "id": eid,
            "value": edge["label"] or "*..*",
            "edge": True,
            "source": src,
            "target": tgt,
            "style": (
                "edgeStyle=orthogonalEdgeStyle;"
                "rounded=0;endArrow=none;strokeColor=#000000;"
            ),
            "geometry": {"relative": True}
        })

    # 4) Wrap in the Draw.io envelope
    return {
        "mxfile": {
            "diagram": [
                {
                    "id": "0",
                    "name": "Page-1",
                    "mxGraphModel": {"root": cells}
                }
            ]
        }
    }


import xml.etree.ElementTree as ET

def generate_drawio_mxfile(model,
                           fkey_mapping=None,
                           cols=3,
                           vertex_size=(140, 60),
                           spacing=(200, 150),
                           margins=(100, 50)):
    """
    Emits a draw.io–native MX-file string from your model dict.
    Correctly sets as="geometry" so draw.io can parse mxGeometry.
    """
    if fkey_mapping is None:
        fkey_mapping = {}

    # 1. build full edge list (explicit + inferred)
    seen = set()
    full_edges = []
    for e in model["edges"]:
        key = (e["from"], e["to"], e["name"])
        if key in seen: continue
        seen.add(key)
        full_edges.append((e["from"], e["to"], e["name"]))
    for v in model["vertices"]:
        src = v["name"]
        for fk in v["fkeys"]:
            tgt = fkey_mapping.get(fk, fk.rsplit("_", 1)[0] + "s")
            key = (src, tgt, fk)
            if key in seen: continue
            seen.add(key)
            full_edges.append((src, tgt, fk))

    # 2. start MX-file
    mxfile = ET.Element(
        "mxfile",
        host="app.diagrams.net",
        modified="2025-09-12T00:00:00.000Z",
        version="15.9.1"
    )
    diagram = ET.SubElement(mxfile, "diagram", id="page1", name="Page-1")
    gm = ET.SubElement(
        diagram, "mxGraphModel",
        dx="0", dy="0", grid="1", gridSize="10", guides="1",
        tooltips="1", connect="1", arrows="1", fold="1",
        page="1", pageScale="1", pageWidth="827", pageHeight="1169", math="0"
    )
    root = ET.SubElement(gm, "root")

    # two required base cells
    ET.SubElement(root, "mxCell", id="0")
    ET.SubElement(root, "mxCell", id="1", parent="0")

    # 3. place vertices
    vid_map = {}
    w, h      = vertex_size
    mx, my    = margins
    sx, sy    = spacing

    for i, v in enumerate(model["vertices"]):
        vid = str(2 + i)
        vid_map[v["name"]] = vid
        row, col = divmod(i, cols)
        x = mx + col * sx
        y = my + row * sy

        # build the visible label
        txt = v["name"]
        if v["fkeys"]:
            txt += "\n" + "\n".join(v["fkeys"])

        cell = ET.SubElement(
            root, "mxCell",
            id=vid,
            value=txt,
            style="rounded=1;whiteSpace=wrap;html=1;fillColor=#dae8fc;strokeColor=#000000;",
            vertex="1",
            parent="1"
        )
        # <-- here we correctly emit as="geometry"
        ET.SubElement(
            cell, "mxGeometry",
            {"x": str(x), "y": str(y), "width": str(w), "height": str(h), "as": "geometry"}
        )

    # 4. place edges
    base = 2 + len(model["vertices"])
    for j, (src, tgt, lbl) in enumerate(full_edges):
        eid = str(base + j)
        s = vid_map.get(src)
        t = vid_map.get(tgt)
        if not s or not t:
            continue
        cell = ET.SubElement(
            root, "mxCell",
            id=eid,
            value=lbl,
            style="edgeStyle=orthogonalEdgeStyle;rounded=0;endArrow=none;strokeColor=#000000;",
            edge="1",
            parent="1",
            source=s,
            target=t
        )
        ET.SubElement(
            cell, "mxGeometry",
            {"relative": "1", "as": "geometry"}
        )

    # 5. serialize
    xml = ET.tostring(mxfile, encoding="utf-8", xml_declaration=True)
    return xml.decode("utf-8")


if __name__ == '__main__':
    settings_path = Path('/home/davidlinux/Documenten/AWV/resources/settings_SyncToArangoDB.json')
    db_settings = load_settings(settings_path)['databases']['prd']
    db_name = db_settings['database']
    username = db_settings['user']
    password = db_settings['password']
    db = ArangoClient().db(db_name, username=username, password=password)

    model = generate_model(db)


    print(json.dumps(model, indent=2))

    # --- USAGE ------------------------------------------------------
    # assume `model` is the dict you printed above
    fkey_overrides = {
        "toezichter_key": "identiteiten",
        "toezichtgroep_key": "toezichtgroepen"
    }

    # drawio_json = generate_drawio_json_with_fkeys(
    #     model,
    #     cols=4,
    #     fkey_mapping=fkey_overrides
    # )
    #
    # print(json.dumps(drawio_json, indent=2))

    xml_content = generate_drawio_mxfile(model, fkey_mapping=fkey_overrides, cols=4)
    with open("diagram.drawio", "w", encoding="utf-8") as f:
        f.write(xml_content)

    print("Wrote diagram.graphml—import this in draw.io")