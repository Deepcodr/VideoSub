from django.contrib import admin
from django.urls import path
from subtimefind import views
from django.conf.urls.static import static
from django.conf import settings

urlpatterns = [
    path('vidupload/', views.vidupload),
    # path('gettime/', views.gettime),
    path('savefile/',views.SaveFile),
    path('searchtime/',views.searchtime),
]+static(settings.MEDIA_URL,document_root=settings.MEDIA_ROOT)
