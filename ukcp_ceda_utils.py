import os
import subprocess
import requests
import getpass

def get_ceda_security_cert(basedir="/tmp",
                           trustroot_dir="/tmp/trustroots",
                           cert_location="/tmp/certs/creds.pem"):
    code_dir = os.path.join(basedir, "ceda_pydap_cert_code")
    if not os.path.exists(code_dir):
        os.makedirs(code_dir)
    cert_dir = os.path.dirname(cert_location)
    if not os.path.exists(cert_dir):
        os.makedirs(cert_dir)

    code_dir = os.path.join(code_dir, "online_ca_client")
    # clone the online_ca_dev repo if it isn't already there
    if not os.path.exists(code_dir):
        os.system("git clone https://github.com/cedadev/online_ca_client {}".format(code_dir))
    p = subprocess.Popen(["./onlineca-get-trustroots-wget.sh",
                          "-U",
                          "https://slcs.ceda.ac.uk/onlineca/trustroots/",
                          "-c",
                          cert_dir,
                          "-b"],
                         cwd=os.path.join(code_dir,
                                          "contrail",
                                          "security",
                                          "onlineca",
                                          "client",
                                          "sh"))
    p.wait()
    # either get CEDA credentials from env variables, or prompt for them.
    if "CEDA_USERNAME" in os.environ.keys():
        ceda_username = os.environ["CEDA_USERNAME"]
    else:
        ceda_username = input("enter CEDA username: ")
    if "CEDA_PASSWORD" in os.environ.keys():
        ceda_password = os.environ["CEDA_PASSWORD"]
    else:
        ceda_password = getpass.getpass("enter CEDA password: ")
    current_dir = os.getcwd()
    os.chdir(os.path.join(code_dir,
                          "contrail",
                          "security",
                          "onlineca",
                          "client",
                          "sh"))
    cmd_string = "echo '{}' | ./onlineca-get-cert-wget.sh -U https://slcs.ceda.ac.uk/onlineca/certificate/ -c {} -l {} -o {} -S".format(ceda_password, trustroot_dir, ceda_username, cert_location)
    os.system(cmd_string)
    os.chdir(current_dir)
    print("Downloaded certificate to {}".format(cert_location))



def download_file(url, cert_filepath="/tmp/certs/creds.pem", output_location="."):
    r = requests.get(url, cert=cert_filepath, verify=False)
    if r.status_code != 200:
        print("Download failed: {}".format(r.text))
        return False
    filename =url.split("/")[-1]
    with open(os.path.join(output_location, filename), "wb") as outfile:
        outfile.write(r.content)
    print("Saved {}".format(os.path.join(output_location, filename)))
    return True


def list_directory(url_base, cert_location="/tmp/certs/creds.pem"):
    """
    Given a URL ending in '/', list all the directories or files in that directory
    """
    if not url_base.endswith("/"):
        url_base += "/"
    if not os.path.exists(cert_location):
        get_ceda_security_cert(cert_location=cert_location)
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.load_cert_chain(cert_location)
    fs = HTTPFileSystem()
    l = fs.ls(url_base, ssl_ctx=ssl_ctx)
    return l

if __name__ == "__main__":
    # example values
    grid_size = "5km"
    freq = "ann"
    ensemble = "01"
    variable = "tas"
    tag = "v20190725"
    time = "198012-200011"

    # example of a full URL
    #example_url = f"http://dap.ceda.ac.uk/badc/ukcp18/data/land-cpm/uk/{grid_size}/rcp85/{ensemble}/{variable}/{freq}/{tag}/{variable}_rcp85_land-cpm_uk_{grid_size}_{ensemble}_{freq}_{time}.nc"
    # same thing without the filename, to try listing the directory
    example_url = f"http://dap.ceda.ac.uk/badc/ukcp18/data/land-cpm/uk/{grid_size}/rcp85/{ensemble}/{variable}/{freq}/{tag}/"

    file_list = list_directory(example_url)
    print(file_list)
