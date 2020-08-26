# Ingestion POC Content Model AMP

## Bulk Update Command

### Sample Manifests

The following Manifest searches for all items in the tree where the NodeRef of the root is `workspace://SpacesStore/91dd5193-c7dd-42cd-803c-91bc47390dda` and changes the title and removes the description

```json
{
    "command": "BULK_UPDATE_BY_QUERY",
    "data": {
        "query": "ANCESTOR:'workspace://SpacesStore/91dd5193-c7dd-42cd-803c-91bc47390dda'",
        "properties": {
            "cm:title": "Changed"
        },
        "deleteProperties": [ "cm:description" ]
    },
    "list": []
}
```

The following Manifest searches for all the items that have the cm:titled aspect on them, removes the `cm:titled` aspect and adds the `sys:temporary` aspect.

```json
{
    "command": "BULK_UPDATE_BY_QUERY",
    "data": {
        "query": "ASPECT:'cm:titled'",
        "aspects": [ "sys:temporary"],
        "removeAspects" [ "cm:titled" ],
    "list": []
}
```