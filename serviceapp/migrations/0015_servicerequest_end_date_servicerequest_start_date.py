# Generated by Django 5.1.2 on 2025-01-18 13:25

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('serviceapp', '0014_alter_servicerequest_status'),
    ]

    operations = [
        migrations.AddField(
            model_name='servicerequest',
            name='end_date',
            field=models.DateTimeField(blank=True, help_text='Дата окончания работ', null=True),
        ),
        migrations.AddField(
            model_name='servicerequest',
            name='start_date',
            field=models.DateTimeField(blank=True, help_text='Дата начала работ', null=True),
        ),
    ]
