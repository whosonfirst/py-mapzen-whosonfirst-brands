import spec	# as in: utils/mk-spec.py > mapzen/whosonfirst/brands/spec.py

# PLEASE FINISH WRITING ME...

def count2size(count):

    # PLEASE USE THE ACTUAL BRANDS SIZE SPEC FOR THIS...
    # (20171129/thisisaaronland)

    count = int(count)
    sz = ""

    if count >= 1:

        if count < 3:
            sz = "O"
        elif count <= 5:
            sz = "XXXS"
        elif count <= 10:
            sz = "XXS"
        elif count <= 20:
            sz = "XS"
        elif count <= 50:
            sz = "S"
        elif count <= 100:
            sz = "M"
        elif count <=  500:
            sz = "L"
        elif count <= 5000:
            sz = "XL"
        elif count <= 10000:
            sz = "XXL"
        else:
            sz = "XXXL"

    return sz
