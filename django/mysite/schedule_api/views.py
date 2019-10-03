from django.shortcuts import render
from rest_framework import viewsets
from .serializers import PracticeDrillSerializer
from .models import PracticeDrill


class PracticeDrillViewSet(viewsets.ModelViewSet):
    queryset = PracticeDrill.objects.all().order_by('name')
    serializer_class = PracticeDrillSerializer
