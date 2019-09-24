from django.db import models


class PracticeDrill(models.Model):
	name = models.CharField(max_length=100)
	skill_level = models.CharField(max_length=30)

	def __str__(self):
		return self.name
