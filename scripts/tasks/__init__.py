from invoke import Collection

from . import remote
from . import gcloud

ns = Collection()
ns.add_collection(gcloud)
ns.add_collection(remote)