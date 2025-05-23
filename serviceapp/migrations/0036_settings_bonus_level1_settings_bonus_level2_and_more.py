# Generated by Django 5.1.2 on 2025-03-26 18:08

from decimal import Decimal
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('serviceapp', '0035_alter_settings_allowed_hosts'),
    ]

    operations = [
        migrations.AddField(
            model_name='settings',
            name='bonus_level1',
            field=models.DecimalField(decimal_places=2, default=Decimal('500'), help_text='Базовый бонус при достижении уровня 1 (Новичок)', max_digits=10),
        ),
        migrations.AddField(
            model_name='settings',
            name='bonus_level2',
            field=models.DecimalField(decimal_places=2, default=Decimal('3000'), help_text='Базовый бонус при достижении уровня 2 (Активный)', max_digits=10),
        ),
        migrations.AddField(
            model_name='settings',
            name='bonus_level3',
            field=models.DecimalField(decimal_places=2, default=Decimal('5000'), help_text='Базовый бонус при достижении уровня 3 (Лид)', max_digits=10),
        ),
        migrations.AddField(
            model_name='settings',
            name='bonus_level4',
            field=models.DecimalField(decimal_places=2, default=Decimal('10000'), help_text='Базовый бонус при достижении уровня 4 (Амбассадор)', max_digits=10),
        ),
        migrations.AddField(
            model_name='settings',
            name='bonus_per_invite',
            field=models.DecimalField(decimal_places=2, default=Decimal('500'), help_text='Сколько начислять дополнительно за каждого приглашённого (для клиента)', max_digits=10),
        ),
        migrations.AddField(
            model_name='settings',
            name='invites_needed_level2',
            field=models.PositiveIntegerField(default=3, help_text='Сколько рефералов (приглашённых) нужно для уровня 2'),
        ),
        migrations.AddField(
            model_name='settings',
            name='invites_needed_level3',
            field=models.PositiveIntegerField(default=10, help_text='Сколько рефералов нужно для уровня 3'),
        ),
        migrations.AddField(
            model_name='settings',
            name='invites_needed_level4',
            field=models.PositiveIntegerField(default=20, help_text='Сколько рефералов нужно для уровня 4 (Амбассадор)'),
        ),
        migrations.AddField(
            model_name='settings',
            name='orders_needed_level4',
            field=models.PositiveIntegerField(default=3, help_text='Сколько завершённых заказов нужно (вместе с invites_needed_level4) для уровня 4'),
        ),
        migrations.AddField(
            model_name='user',
            name='client_level',
            field=models.PositiveSmallIntegerField(default=0, help_text='Уровень клиента (0 — пока не присвоен)'),
        ),
        migrations.AlterField(
            model_name='master',
            name='rating',
            field=models.DecimalField(decimal_places=2, default=5, max_digits=5),
        ),
    ]
