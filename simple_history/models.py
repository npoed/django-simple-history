# coding: utf-8
from __future__ import unicode_literals

import copy
import importlib
import threading

from django.db import models, router
from django.db.models.fields.proxy import OrderWrt
from django.conf import settings
from django.contrib import admin
from django.utils import six
from django.utils.encoding import python_2_unicode_compatible
from django.utils.encoding import smart_text
from django.utils.timezone import now
from django.utils.translation import string_concat
from simple_history import register
from django.db.models.query import Q

try:
    from django.apps import apps
except ImportError:  # Django < 1.7
    from django.db.models import get_app
try:
    from south.modelsinspector import add_introspection_rules
except ImportError:  # south not present
    pass
else:  # south configuration for CustomForeignKeyField
    add_introspection_rules(
        [], ["^simple_history.models.CustomForeignKeyField"])

from . import exceptions
from .manager import HistoryDescriptor

registered_models = {}
future_register_models = []
registered_historical_models = {}
fake_m2m_models = {}


class HistoricalRecords(object):
    thread = threading.local()

    def __init__(self, verbose_name=None, bases=(models.Model,),
                 user_related_name='+', table_name=None, inherit=False,
                 is_m2m=False):
        self.user_set_verbose_name = verbose_name
        self.user_related_name = user_related_name
        self.table_name = table_name
        self.inherit = inherit
        self.is_m2m = is_m2m
        try:
            if isinstance(bases, six.string_types):
                raise TypeError
            self.bases = tuple(bases)
        except TypeError:
            raise TypeError("The `bases` option must be a list or a tuple.")

    def contribute_to_class(self, cls, name):
        self.manager_name = name
        self.module = cls.__module__
        self.cls = cls
        models.signals.class_prepared.connect(self.finalize, weak=False)
        self.add_extra_methods(cls)
        self.setup_m2m_history(cls)

    def add_extra_methods(self, cls):
        def save_without_historical_record(self, *args, **kwargs):
            """
            Save model without saving a historical record

            Make sure you know what you're doing before you use this method.
            """
            self.skip_history_when_saving = True
            try:
                ret = self.save(*args, **kwargs)
            finally:
                del self.skip_history_when_saving
            return ret

        setattr(cls, 'save_without_historical_record',
                save_without_historical_record)

    def setup_m2m_history(self, cls):
        m2m_history_fields = [m2m.name for m2m in cls._meta.many_to_many]
        for attr in dir(cls):
            if hasattr(cls, attr) and hasattr(getattr(cls, attr), 'related') and getattr(cls,
                                                                                         attr).related.many_to_many:
                m2m_history_fields.append(attr)
        if m2m_history_fields:
            assert (isinstance(m2m_history_fields, list) or isinstance(m2m_history_fields,
                                                                       tuple)), 'm2m_history_fields must be a list or tuple'
        for field_name in m2m_history_fields:
            if hasattr(getattr(cls, field_name), 'field'):
                field = getattr(cls, field_name).field
            else:
                field = getattr(cls, field_name).related.field
            assert isinstance(field, models.fields.related.ManyToManyField), (
                '%s must be a ManyToManyField' % field_name)
            if field.rel.related_model._meta.db_table in registered_models \
                and field.rel.to._meta.db_table in registered_models:
                if not sum([isinstance(item, HistoricalRecords) for item in field.rel.through.__dict__.values()]) and \
                    not field.rel.through._meta.db_table in registered_models:
                    register(field.rel.through, is_m2m=True)

    def finalize(self, sender, **kwargs):
        try:
            hint_class = self.cls
        except AttributeError:  # called via `register`
            pass
        else:
            if hint_class is not sender:  # set in concrete
                if not (self.inherit and issubclass(sender, hint_class)):  # set in abstract
                    return
        if hasattr(sender._meta, 'simple_history_manager_attribute'):
            raise exceptions.MultipleRegistrationsError('{}.{} registered multiple times for history tracking.'.format(
                sender._meta.app_label,
                sender._meta.object_name,
            ))
        history_model = self.create_history_model(sender)
        module = importlib.import_module(self.module)
        setattr(module, history_model.__name__, history_model)

        # The HistoricalRecords object will be discarded,
        # so the signal handlers can't use weak references.
        # models.signals.pre_save.connect(self.pre_save, sender=sender,
        #                                 weak=False)
        models.signals.post_save.connect(self.post_save, sender=sender,
                                         weak=False)
        models.signals.post_delete.connect(self.post_delete, sender=sender,
                                           weak=False)
        models.signals.m2m_changed.connect(self.m2m_changed, sender=sender,
                                           weak=False)

        descriptor = HistoryDescriptor(history_model)
        setattr(sender, self.manager_name, descriptor)
        sender._meta.simple_history_manager_attribute = self.manager_name

    def create_history_model(self, model):
        """
        Creates a historical model to associate with the model provided.
        """
        attrs = {'__module__': self.module}

        app_module = '%s.models' % model._meta.app_label
        if model.__module__ != self.module:
            # registered under different app
            attrs['__module__'] = self.module
        elif app_module != self.module:
            try:
                # Abuse an internal API because the app registry is loading.
                app = apps.app_configs[model._meta.app_label]
            except NameError:  # Django < 1.7
                models_module = get_app(model._meta.app_label).__name__
            else:
                models_module = app.name
            attrs['__module__'] = models_module

        fields = self.copy_fields(model)
        attrs.update(fields)
        attrs.update(self.get_extra_fields(model, fields))
        # type in python2 wants str as a first argument
        attrs.update(Meta=type(str('Meta'), (), self.get_meta_options(model)))
        if self.table_name is not None:
            attrs['Meta'].db_table = self.table_name
        name = 'Historical%s' % model._meta.object_name
        registered_models[model._meta.db_table] = model
        historical_model = python_2_unicode_compatible(type(str(name), self.bases, attrs))
        historical_model.is_m2m = self.is_m2m
        registered_historical_models[model.__name__] = historical_model
        return historical_model

    def copy_fields(self, model):
        """
        Creates copies of the model's original fields, returning
        a dictionary mapping field name to copied field object.
        """
        fields = {}
        for field in model._meta.fields:
            field = copy.copy(field)
            try:
                field.remote_field = copy.copy(field.remote_field)
            except AttributeError:
                field.rel = copy.copy(field.rel)
            if isinstance(field, OrderWrt):
                # OrderWrt is a proxy field, switch to a plain IntegerField
                field.__class__ = models.IntegerField
            if isinstance(field, models.ForeignKey):
                old_field = field
                field_arguments = {'db_constraint': False}
                if (getattr(old_field, 'one_to_one', False) or
                        isinstance(old_field, models.OneToOneField)):
                    FieldType = models.ForeignKey
                else:
                    FieldType = type(old_field)
                if getattr(old_field, 'to_fields', []):
                    field_arguments['to_field'] = old_field.to_fields[0]
                if getattr(old_field, 'db_column', None):
                    field_arguments['db_column'] = old_field.db_column
                field = FieldType(
                    old_field.rel.to,
                    related_name='+',
                    null=True,
                    blank=True,
                    primary_key=False,
                    db_index=True,
                    serialize=True,
                    unique=False,
                    on_delete=models.DO_NOTHING,
                    **field_arguments
                )
                field.name = old_field.name
            else:
                transform_field(field)
            fields[field.name] = field
        return fields

    def get_extra_fields(self, model, fields):
        """Return dict of extra fields added to the historical record model"""

        user_model = getattr(settings, 'AUTH_USER_MODEL', 'auth.User')

        @models.permalink
        def revert_url(self):
            """URL for this change in the default admin site."""
            opts = model._meta
            app_label, model_name = opts.app_label, opts.model_name
            return ('%s:%s_%s_simple_history' %
                    (admin.site.name, app_label, model_name),
                    [getattr(self, opts.pk.attname), self.history_id])

        def get_instance(self):
            return model(**{
                field.attname: getattr(self, field.attname)
                for field in fields.values()
                })

        extra_fields = {
            'history_id': models.AutoField(primary_key=True),
            'history_date': models.DateTimeField(),
            'history_user': models.ForeignKey(
                user_model, null=True, related_name=self.user_related_name,
                on_delete=models.SET_NULL),
            'history_type': models.CharField(max_length=1, choices=(
                ('+', 'Created'),
                ('~', 'Changed'),
                ('-', 'Deleted'),
            )),
            'history_object': HistoricalObjectDescriptor(model),
            'instance': property(get_instance),
            'instance_type': model,
            'revert_url': revert_url,
            '__str__': lambda self: '%s as of %s' % (self.history_object,
                                                     self.history_date)
        }
        if self.is_m2m:
            for field in model._meta.fields:
                if isinstance(field, models.ForeignKey) and field.rel.to.__name__ in registered_historical_models:
                    extra_fields.update({
                        'history_{}'.format(field.rel.to.__name__): models.ForeignKey(
                            registered_historical_models[field.rel.to.__name__],
                            null=True, default=None)
                    })
        # else:
        #     for fld in model._meta.fields:
        #         if isinstance(fld, models.ForeignKey) and fld.rel.model in future_register_models:
        #             extra_fields.update({
        #                 'history_{}'.format(fld.rel.model.__name__): models.ForeignKey(
        #                     u"{}.Historical{}".format(fld.rel.model._meta.app_label, fld.rel.model.__name__),
        #                     null=True, default=None)
        #             })

        return extra_fields

    def get_meta_options(self, model):
        """
        Returns a dictionary of fields that will be added to
        the Meta inner class of the historical record model.
        """
        meta_fields = {
            'ordering': ('-history_date', '-history_id'),
            'get_latest_by': 'history_date',
        }
        if self.user_set_verbose_name:
            name = self.user_set_verbose_name
        else:
            name = string_concat('historical ',
                                 smart_text(model._meta.verbose_name))
        meta_fields['verbose_name'] = name
        return meta_fields

    def post_save(self, instance, created, **kwargs):
        if not created and hasattr(instance, 'skip_history_when_saving'):
            return
        if not kwargs.get('raw', False):
            self.create_historical_record(instance, created and '+' or '~')

    def pre_save(self, instance, **kwargs):
        if not self.is_m2m and instance.pk is not None and \
            not registered_historical_models[instance._meta.model.__name__].objects.filter(id=instance.id).exists() and \
            not kwargs.get('raw', False):
            self.create_historical_record(instance._meta.model.objects.get(pk=instance.pk), '+')

    def post_delete(self, instance, **kwargs):
        # При удалении не будет создаваться historical_record с типом "-"
        # if self.is_m2m:
        self.remove_historical_record(instance)
        # else:
        #     self.create_historical_record(instance, '-')

    def m2m_changed(self, action, instance, sender, **kwargs):
        source_field_name, target_field_name = None, None
        for field_name, field_value in sender.__dict__.items():
            if isinstance(field_value, models.fields.related.ReverseSingleRelatedObjectDescriptor):
                if field_value.field.related.model == kwargs['model']:
                    target_field_name = field_name
                elif field_value.field.related.model == type(instance):
                    source_field_name = field_name
        items = sender.objects.filter(**{source_field_name: instance})
        if kwargs.get('pk_set'):
            items = items.filter(**{target_field_name + '__id__in': kwargs['pk_set']})
        for item in items:
            if action == 'post_add':
                if hasattr(item, 'skip_history_when_saving'):
                    return
                self.create_historical_record(item, '+')
            elif action in ['pre_remove', 'pre_clear']:
                self.remove_historical_record(item)

    def create_historical_record(self, instance, history_type):
        if registered_historical_models[instance._meta.model.__name__].is_m2m:
            for field in instance._meta.fields:
                if isinstance(field, models.ForeignKey) and field.rel.to.__name__ in registered_historical_models:
                    if not registered_historical_models[field.rel.to.__name__].objects.all().filter(
                        id=getattr(instance, field.name).id).exists():
                        self.create_historical_record(getattr(instance, field.name), '+')
        history_date = getattr(instance, '_history_date', now())
        history_user = self.get_history_user(instance)
        manager = getattr(instance, self.manager_name)
        attrs = {}
        for field in instance._meta.fields:
            attrs[field.attname] = getattr(instance, field.attname)
            if registered_historical_models[instance._meta.model.__name__].is_m2m:
                if isinstance(field, models.ForeignKey):
                    real_model_name = field.rel.to.__name__
                    if real_model_name not in registered_historical_models:
                        continue
                    real_record_id = getattr(instance, field.attname)
                    history_records = registered_historical_models[real_model_name].objects.filter(id=real_record_id)
                    if history_records.exists():
                        attrs['history_{}'.format(real_model_name)] = history_records.latest('history_date')

        if registered_historical_models[instance._meta.model.__name__].is_m2m:
            query_list = []
            real_field_names = [f.name for f in instance._meta.fields]
            for field in registered_historical_models[instance._meta.model.__name__]._meta.fields:
                if isinstance(field, models.ForeignKey) and field.name not in real_field_names and \
                        field.name != 'history_user':
                    query_list.append(Q(**{field.name: attrs[field.name]}))
                query = reduce(lambda x, y: x & y, query_list, Q())
            if registered_historical_models[instance._meta.model.__name__].objects.filter(query).exists():
                return

        manager.create(history_date=history_date, history_type=history_type, history_user=history_user, **attrs)

        if registered_historical_models[instance._meta.model.__name__].is_m2m:
            return

        # if history_type == '+':
        #     for f_key in instance._meta.related_objects:
        #         real_model_name = f_key.related_model.__name__
        #         if real_model_name not in registered_historical_models:
        #             continue
        #         # Выбираем объекты, ссылающиеся на данную модель
        #         q_dict = {f_key.field.name: instance}
        #         query = f_key.related_model.objects.all().filter(**q_dict)
        #         for q in query:
        #             if not registered_historical_models[real_model_name].objects.all().filter(id=q.id).exists():
        #                 self.create_historical_record(q, '+')
        #     for m2m in instance._meta.many_to_many:
        #         if m2m.related_model.__name__ in registered_historical_models:
        #             for q in getattr(instance, m2m.name).all():
        #                 if not registered_historical_models[m2m.related_model.__name__].objects.filter(
        #                     id=q.id).exists():
        #                     self.create_historical_record(q, '+')

        # Смотрим, есть ли наша модель в fake m2m
        for f_m2m_key, f_m2m_value in fake_m2m_models.items():
            if f_m2m_key[0] is instance._meta.model:
                to_inst = getattr(instance, f_m2m_key[2])
                if to_inst is None:
                    continue
                to_hist_insts = registered_historical_models[to_inst._meta.model.__name__].objects.filter(
                    id=to_inst.id)
                from_hist_items = registered_historical_models[instance._meta.model.__name__].objects.filter(
                    id=instance.id).order_by('-history_date')
                if to_hist_insts.exists() and from_hist_items.exists():
                    if from_hist_items.count() > 1:
                        prev_hist_item = from_hist_items[1]
                        buf_fake_m2m_attrs = {
                            registered_historical_models[to_inst._meta.model.__name__].__name__: to_hist_insts.latest('history_date'),
                            registered_historical_models[instance._meta.model.__name__].__name__: prev_hist_item,
                        }
                        f_m2m_key[1].objects.filter(**buf_fake_m2m_attrs).delete()
                    buf_fake_m2m_attrs = {
                        registered_historical_models[to_inst._meta.model.__name__].__name__: to_hist_insts.latest('history_date'),
                        registered_historical_models[instance._meta.model.__name__].__name__: from_hist_items[0],
                    }
                    if not f_m2m_key[1].objects.filter(**buf_fake_m2m_attrs).exists():
                        f_m2m_key[1](**buf_fake_m2m_attrs).save()

            elif f_m2m_value[0] is instance._meta.model:
                for from_inst in getattr(instance, f_m2m_value[2]).all():
                    fake_m2m_model = f_m2m_value[1]
                    from_hist_model = registered_historical_models[f_m2m_key[0].__name__]
                    to_hist_model = registered_historical_models[f_m2m_value[0].__name__]
                    to_hist_insts = to_hist_model.objects.filter(id=instance.id)
                    from_hist_insts = from_hist_model.objects.filter(id=from_inst.id)
                    if from_hist_insts.exists() and to_hist_insts.exists():
                        fake_m2m_attrs = {
                            from_hist_model.__name__: from_hist_insts.latest('history_date'),
                            to_hist_model.__name__: to_hist_insts.latest('history_date'),
                        }
                        if not fake_m2m_model.objects.filter(**fake_m2m_attrs).exists():
                            fake_m2m_model(**fake_m2m_attrs).save()


    def create_fake_m2m(self, model):
        if not self.is_m2m:
            for attr in dir(model):
                if hasattr(model, attr) and hasattr(getattr(model, attr), 'field'):
                    fld = getattr(model, attr).field
                    if fld.is_relation and not fld.many_to_many and \
                            fld.rel.model.__name__ in registered_historical_models and \
                            not registered_historical_models[fld.rel.model.__name__].is_m2m:
                        to_model = fld.rel.model
                        if fld.rel.related_name == '+':
                            continue
                        if fld.rel.related_name is not None:
                            to_name = fld.rel.related_name
                        else:
                            to_name = '{}_set'.format(fld.rel.name)
                        from_model = model
                        from_name = fld.name
                    else:
                        continue
                elif hasattr(model, attr) and hasattr(getattr(model, attr), 'related'):
                    rel = getattr(model, attr).related
                    if rel.is_relation and not rel.field.many_to_many and \
                            rel.field.model.__name__ in registered_historical_models and \
                            not registered_historical_models[rel.field.model.__name__].is_m2m:
                        from_model = rel.field.model
                        from_name = rel.field.name
                        to_model = model
                        to_name = attr
                    else:
                        continue
                else:
                    continue
                to_hist_model = registered_historical_models[to_model.__name__]
                from_hist_model = registered_historical_models[from_model.__name__]
                attrs = {
                    u'__module__': from_model.__module__,
                    'history_id': models.AutoField(primary_key=True),
                    from_hist_model.__name__: models.ForeignKey(to=from_hist_model, related_name='+'),
                    to_hist_model.__name__: models.ForeignKey(to=to_hist_model, related_name='+'),
                    '__str__': lambda self: '%s' % self.__name__
                }
                name = '{}_{}_fake'.format(from_hist_model.__name__, to_hist_model.__name__)
                historical_model = python_2_unicode_compatible(type(str(name), self.bases, attrs))
                fake_m2m_models[(from_model, historical_model, from_name)] = (to_model, historical_model, to_name)

    def get_history_user(self, instance):
        """Get the modifying user from instance or middleware."""
        try:
            return instance._history_user
        except AttributeError:
            try:
                if self.thread.request.user.is_authenticated():
                    return self.thread.request.user
                return None
            except AttributeError:
                return None

    def remove_historical_record(self, item):
        if self.is_m2m:
            query_list = []
            for field in item._meta.fields:
                if isinstance(field, models.ForeignKey):
                    real_model_name = field.rel.to.__name__
                    if real_model_name not in registered_historical_models:
                        return
                    historical_rel_instances = registered_historical_models[real_model_name].objects.filter(
                        id=getattr(item, field.attname))
                    if historical_rel_instances.exists():
                        query_list.append(
                            Q(**{'history_{}'.format(real_model_name): historical_rel_instances.latest('history_date')}))
            query = reduce(lambda x, y: x & y, query_list, Q())
            res_query = registered_historical_models[item._meta.model.__name__].objects.filter(query)
            if res_query.exists():
                res_query.delete()
        else:
            history_model = registered_historical_models[item._meta.model.__name__]
            last_history_item = history_model.objects.filter(id=item.id)\
                .latest('history_date')
            for f_m2m_key, f_m2m_value in fake_m2m_models.items():
                buf = []
                if f_m2m_key[0] is item._meta.model:
                    res = f_m2m_key[1].objects.filter(**{history_model.__name__: last_history_item})
                    for itm in res:
                        another_model = registered_historical_models[f_m2m_value[0].__name__]
                        another_item = getattr(itm, another_model.__name__)
                        another_latest_instance = another_model.objects.filter(**{'id': another_item.id}).latest('history_date')
                        if another_latest_instance == another_item:
                            buf.append(itm)
                elif f_m2m_value[0] is item._meta.model:
                    res = f_m2m_value[1].objects.filter(**{history_model.__name__: last_history_item})
                    for itm in res:
                        another_model = registered_historical_models[f_m2m_key[0].__name__]
                        another_item = getattr(itm, another_model.__name__)
                        another_latest_instance = another_model.objects.filter(**{'id': another_item.id}).latest(
                            'history_date')
                        if another_latest_instance == another_item:
                            buf.append(itm)
                for bf in range(len(buf)):
                    buf[bf].delete()


def transform_field(field):
    """Customize field appropriately for use in historical model"""
    field.name = field.attname
    if isinstance(field, models.AutoField):
        field.__class__ = convert_auto_field(field)

    elif isinstance(field, models.FileField):
        # Don't copy file, just path.
        field.__class__ = models.TextField

    # Historical instance shouldn't change create/update timestamps
    field.auto_now = False
    field.auto_now_add = False

    if field.primary_key or field.unique:
        # Unique fields can no longer be guaranteed unique,
        # but they should still be indexed for faster lookups.
        field.primary_key = False
        field._unique = False
        field.db_index = True
        field.serialize = True


def convert_auto_field(field):
    """Convert AutoField to a non-incrementing type

    The historical model gets its own AutoField, so any existing one
    must be replaced with an IntegerField.
    """
    connection = router.db_for_write(field.model)
    if settings.DATABASES[connection]['ENGINE'] in ('django_mongodb_engine',):
        # Check if AutoField is string for django-non-rel support
        return models.TextField
    return models.IntegerField


class HistoricalObjectDescriptor(object):
    def __init__(self, model):
        self.model = model

    def __get__(self, instance, owner):
        values = (getattr(instance, f.attname)
                  for f in self.model._meta.fields)
        return self.model(*values)
