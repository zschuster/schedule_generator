from django.urls import include, path
from rest_framework import routers
from . import views


router = routers.DefaultRouter()
router.register(r'practice_drills', views.PracticeDrillViewSet)

# wire up the API using automatic URL routing
# we also include login URLs for the browsable API

urlpatterns = [
    path('', include(router.urls)),
    path('api-auth/', include('rest_framework.urls',
                              namespace='rest_framework'))
]
