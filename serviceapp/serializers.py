from rest_framework import serializers
from .models import User, Master, City

class UserRegistrationSerializer(serializers.Serializer):
    name = serializers.CharField(required=True)
    phone = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    telegram_id = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    email = serializers.EmailField(required=False, allow_blank=True, allow_null=True)
    role = serializers.ChoiceField(choices=User.ROLE_CHOICES)
    city_name = serializers.CharField(required=False, allow_blank=True)

    def create(self, validated_data):
        role = validated_data.get('role', 'Client')
        city_name = validated_data.pop('city_name', None)

        # Создаем пользователя
        user = User.objects.create(**validated_data)
        
        # Если роль мастер, создадим мастера
        if role == 'Master':
            if city_name:
                city, _ = City.objects.get_or_create(name=city_name)
            else:
                raise serializers.ValidationError("City name is required for Master registration.")
            
            Master.objects.create(user=user, city=city)

        return user
