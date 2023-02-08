# -*- coding: utf-8 -*-
# Author: Lx
# Date: 2021/3/16 18:20

from django.urls import path
from django.http import HttpResponse
from api.views import trace_source


def index(request):
    return HttpResponse("Hello, world. You're at Project Name.")


urlpatterns = [
    path('', index, name='index'),
    path('trace-source', trace_source.trace_source),    # 气源比率 气体组分比率 单位热值  (体积/质量)
]
