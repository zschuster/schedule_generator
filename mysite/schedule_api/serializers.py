from rest_framework import serializers
from .models import PracticeDrill


class PracticeDrillSerializer(serializers.HyperlinkedModelSerializer):

	class Meta:
		model = PracticeDrill
		fields = ('Name', 'skill_level')