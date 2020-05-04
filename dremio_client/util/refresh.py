
def refresh_vds_reflection_by_path(client, path=None):
    """
    By providing a path from VDS the reflection of that vds will be refreshed
    Script will find which pds is responsible for the reflection and
    trigger the refresh based on pds

    :param path: list ['space', 'folder', 'vds']
    """
    datasets = []
    datasets.append(client.catalog_item(cid=None, path=path))
    refresh_by_path(client, datasets)


def refresh_by_path(client, datasets):
    for dataset in datasets:
        response = client.graph(dataset['id'])
        parents(client, response['parents'])


def parents(client, parents):
    datasets = []
    for parent in parents:
        if (parent['datasetType'] == 'VIRTUAL'):
            datasets.append({"id": parent['id']})
        else:
            client.refresh_pds(client.catalog_item(cid=parent['id'], path=None)['id'])
        break
    refresh_by_path(client, datasets)
