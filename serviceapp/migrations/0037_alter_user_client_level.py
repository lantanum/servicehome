# Generated by Django 5.1.2 on 2025-03-26 18:23

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('serviceapp', '0036_settings_bonus_level1_settings_bonus_level2_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='user',
            name='client_level',
            field=models.PositiveSmallIntegerField(default=1, help_text='Уровень клиента (0 — пока не присвоен)'),
        ),
    ]
