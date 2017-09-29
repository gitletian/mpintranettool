#coding=utf-8

__author__ = 'Rich'

from django import template
register = template.Library()


@register.filter
def icon(value):
    return '<i class="fa fa-caret-{0} m-l-xs f16"></i>'.format('down' if value == '-' else 'up')

