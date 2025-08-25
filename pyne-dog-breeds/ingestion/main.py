from extract_dogs import extract_dogs

# Cloud Functions entry poi
def main(request):
    return extract_dogs(request)