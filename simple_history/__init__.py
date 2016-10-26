from __future__ import unicode_literals
from django.db.models.query import Q
from django.db.models.loading import get_model
from django.db.models.signals import post_save, m2m_changed

__version__ = '1.8.1'


def register(
        model, app=None, manager_name='history', records_class=None,
        table_name=None, **records_config):
    """
    Create historical model for `model` and attach history manager to `model`.

    Keyword arguments:
    app -- App to install historical model into (defaults to model.__module__)
    manager_name -- class attribute name to use for historical manager
    records_class -- class to use for history relation (defaults to
        HistoricalRecords)
    table_name -- Custom name for history table (defaults to
        'APPNAME_historicalMODELNAME')

    This method should be used as an alternative to attaching an
    `HistoricalManager` instance directly to `model`.
    """
    from . import models

    if records_class is None:
        records_class = models.HistoricalRecords

    records = records_class(**records_config)
    records.manager_name = manager_name
    records.table_name = table_name
    records.module = app and ("%s.models" % app) or model.__module__
    records.add_extra_methods(model)
    records.finalize(model)
    models.registered_models[model._meta.db_table] = model
    records.setup_m2m_history(model)
    records.create_fake_m2m(model)


def register_model_list(model_list):
    from . import models

    models.future_register_models = model_list
    for mdl in model_list:
        register(mdl)


def init_historical_records_from_model(model, is_m2m=False):
    from . import models as historical_models
    from django.db import models

    real_instances = model.objects.all()
    real_instance_fields = [f.name for f in model._meta.fields]
    for real_instance in real_instances:
        query_list = []
        for field in real_instance_fields:
            query_list.append(Q(**{field: getattr(real_instance, field)}))
        query = reduce(lambda x, y: x & y, query_list, Q())
        if historical_models.registered_historical_models[model.__name__].objects.filter(query).exists():
            continue
        else:
            if is_m2m:
                attrs = []
                for field in model._meta.fields:
                    if isinstance(field, models.ForeignKey):
                        attrs.append(field)
                        if attrs.__len__() == 2:
                            break
                m2m_changed.send(model, instance=getattr(real_instance, attrs[0].name), model=attrs[1].rel.to,
                                 action='post_add')
            else:
                post_save.send(model, instance=real_instance, created=True, weak=False)


def init_historical_records():
    from . import models

    for model_name, hist_model in models.registered_historical_models.items():
        if not hist_model.is_m2m:
            init_historical_records_from_model(get_model(hist_model._meta.app_label, model_name))

    for model_name, hist_model in models.registered_historical_models.items():
        if hist_model.is_m2m:
            init_historical_records_from_model(get_model(hist_model._meta.app_label, model_name), is_m2m=True)
