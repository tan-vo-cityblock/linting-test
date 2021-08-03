import logging
from typing import Callable, Dict, Optional

from cf.gcs.effects import GCSBlobPathEffect
from cf.gcs.events import GCSBlobPath
from cf.gcs.types import GCSBlobPathMatcher
from cityblock import cloud_composer

# TODO cf shouldn't import from cityblock
from cityblock.cloud_composer import CloudComposerCluster


class DagTriggerEffect(GCSBlobPathEffect):
    dag_id: str
    cluster: CloudComposerCluster

    def __init__(
        self, dag_id: str, cluster: Optional[CloudComposerCluster] = cloud_composer.prod
    ) -> None:
        super().__init__()
        self.dag_id = dag_id
        self.cluster = cluster

    def precheck(self, matcher: GCSBlobPathMatcher, blob_path: GCSBlobPath) -> bool:
        raise NotImplementedError

    def conf(self, matcher: GCSBlobPathMatcher, blob_path: GCSBlobPath) -> Dict:
        raise NotImplementedError

    def effect(self, matcher: GCSBlobPathMatcher, blob_path: GCSBlobPath) -> None:
        if self.precheck(matcher, blob_path):
            logging.info(
                f"triggering dag={self.dag_id} with fullpath:{blob_path.fullpath}"
            )
            cloud_composer.prod.dag(self.dag_id).trigger(
                conf=self.conf(matcher, blob_path)
            )
        else:
            logging.info(
                f"failed precheck for triggering dag={self.dag_id} with fullpath: {blob_path.fullpath}"
            )


class LiteDagTriggerEffect(DagTriggerEffect):
    dag_id: str
    precheck_fn: Optional[Callable[[GCSBlobPath], bool]]
    conf_fn: Optional[Callable[[GCSBlobPath], Dict]]

    def __init__(
        self,
        dag_id: str,
        conf_fn: Optional[Callable[[GCSBlobPathMatcher, GCSBlobPath], Dict]],
        precheck_fn: Optional[
            Callable[[GCSBlobPathMatcher, GCSBlobPath], bool]
        ] = lambda bp: True,
    ) -> None:
        super().__init__(dag_id=dag_id)
        self.dag_id = dag_id
        self.precheck_fn = precheck_fn
        self.conf_fn = conf_fn

    def precheck(self, matcher: GCSBlobPathMatcher, blob_path: GCSBlobPath) -> bool:
        return self.precheck_fn(matcher, blob_path)

    def conf(self, matcher: GCSBlobPathMatcher, blob_path: GCSBlobPath) -> Dict:
        return self.conf_fn(matcher, blob_path)


"""utility function for making dag trigger effects inline

Args:
    dag_id (str): dag_id to be triggered
    precheck_fn (Optional[Callable[[GCSBlobPath], bool]], optional): 
        a function that will be used to check of all necessarily
        external states are in place to actually trigger the dag. 
        Defaults to lambda bp: True.
    conf_fn (Optional[Callable[[GCSBlobPath], Dict]], optional): 
        if precheck is successful, use conf_fn(blob_path) to generate
        the conf object to pass to the dag. Hint: this is usually 
        {"date": "20210203"} where 20210203 is the date of the new
        gcs file that triggered the function  Defaults to _default_conf_fn.

Returns:
    LiteDagTriggerEffect: 
"""
dag_trigger = LiteDagTriggerEffect
