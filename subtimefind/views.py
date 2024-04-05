from django.shortcuts import render
from django.http import HttpResponseRedirect, JsonResponse
import os
from django.conf import settings
#celery -A VideoSub.celery_conf worker --pool=solo -l info  
#java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb

from django.core.files.storage import default_storage
from .tasks import getsubs, uploadtos3,gettime ,getvidnames

# Create your views here.
def vidupload(request):
    if request.method=='GET':
        return render(request,'fileupload.html')

def searchtime(request):
    if request.method=='GET':
        videos=getvidnames.apply_async(ignore_result=False).get()
        vidlist=list(videos)
        time=[]
        context={
            "vidlist":videos,
            "timelist":time
        }
        return render(request,'searchtime.html',context)
    elif request.method=='POST':
        query=request.POST["query"]
        filename=request.POST["file"]
        query=query.upper()
        time=gettime.apply_async((query,filename),ignore_result=False).get()
        print(time)
        videos=getvidnames.apply_async(ignore_result=False).get()
        vidlist=list(videos)
        context={
            "timelist":time,
            "vidlist":vidlist
        }
        # return HttpResponseRedirect('/searchtime/')
        return render(request,'searchtime.html',context)
        # return JsonResponse(time,safe=False)
        # json_string = json.dumps(response['Items'])
        # return JsonResponse("Hello",safe=False)
        # return JsonResponse(json_string,safe=False)

def SaveFile(request):
    if request.method=='POST':
        file=request.FILES["file"]
        fname=str(file.name)
        fname=fname.replace(" ","")
        fname=fname.replace("(","")
        fname=fname.replace(")","")
        print(fname)
        file_name=default_storage.save(fname,file)
        print(file_name)
        runfile=os.path.join(settings.BASE_DIR,f"media\\{file_name}")
        os.system(f"ccextractorwinfull {runfile} -out=ttxt")
        getsubs.delay(file_name)
        return HttpResponseRedirect('/searchtime/')