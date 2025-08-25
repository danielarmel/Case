from ingestion.extract_dogs import extract_dogs

# Cloud Functions entry poin
def main(request):
    return extract_dogs(request)