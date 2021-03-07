import os, sys
from bs4 import BeautifulSoup as bs
import urllib.request as req
from zipfile import ZipFile
import pandas as pd
import lxml.etree as etree
import boto3
import logging

logger = logging.getLogger("app.py")

def upload_to_s3(csv_folder):
    try:
        ACCESS_KEY = ""
        SECRETE_KEY = ""
        BUKET_NAME = ""
        S3 = boto3.client("s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRETE_KEY)
        csv_folder = csv_folder
        s3_folder = "CSVFILES"
        walks = os.walk(csv_folder)
        for source, dir, files in walks:
            for filename in files:
                file = os.path.join(source, filename)
                rfilepath = os.path.relpath(file, csv_folder)
                s3_file = os.path.join(s3_folder, rfilepath)
                try:
                    S3.head_object(Bucket = BUKET_NAME, key = s3_file)
                except:
                    S3.upload_file(file, BUKET_NAME, s3_file)
        return "Uploaded Sucess"
    except Exception as e:
        logger.error("Error in upload_to_s3 : " + "\n" + str(repr(e)) + " line number " + str(sys.exc_info()[2].tb_lineno))


def parser_xml(bs_content):
    try:
        FinInstrmGnlAttrbts_id = []
        FinInstrmGnlAttrbts_FullNm = []
        FinInstrmGnlAttrbts_ClssfctnTp =[]
        FinInstrmGnlAttrbts_CmmdtyDerivInd = []
        FinInstrmGnlAttrbts_NtnlCcy = []
        issr = []

        for x in bs_content.findAll("doc"):
            if x.find("str", {"name": "file_type"}).text == "DLTINS":
                zip_link = x.find("str", {"name": "download_link"}).text
                req.urlretrieve(zip_link, "./zipfiles/{}".format(zip_link.rsplit("/", 1)[1]))
                zip = ZipFile("./zipfiles/{}".format(zip_link.rsplit("/", 1)[1]))
                zip.extractall("./zipfiles/{}".format(zip_link.rsplit("/", 1)[1].rsplit(".", 1)[0]))
                if os.path.exists("./zipfiles/{}".format(zip_link.rsplit("/", 1)[1].rsplit(".", 1)[0])):
                    xmlfiles = os.listdir("./zipfiles/{}".format(zip_link.rsplit("/", 1)[1].rsplit(".", 1)[0]))
                    for files in xmlfiles:
                        newfile = "./zipfiles/{}/{}".format(zip_link.rsplit("/", 1)[1].rsplit(".", 1)[0], files)
                        lxml = etree.parse(newfile)
                        lxml = etree.tostring(lxml, encoding="unicode", pretty_print=True)
                        savexml = open(newfile, "w")
                        savexml.write(lxml)
                        savexml.close()
                        with open(newfile, "r") as file:
                            content = "".join(file.readlines())
                            bs_content2 = bs(content, "xml")
                        for x in bs_content2.findAll("FinInstrmGnlAttrbts"):
                            FinInstrmGnlAttrbts_id.append(x.find("Id").text)
                            FinInstrmGnlAttrbts_FullNm.append(x.find("FullNm").text)
                            FinInstrmGnlAttrbts_ClssfctnTp.append(x.find("ClssfctnTp").text)
                            FinInstrmGnlAttrbts_CmmdtyDerivInd.append(x.find("CmmdtyDerivInd").text)
                            FinInstrmGnlAttrbts_NtnlCcy.append(x.find("NtnlCcy").text)

                        Issr = bs_content2.findAll("Issr")
                        for t in Issr:
                            issr.append(t.text)

                    df_data = {"FinInstrmGnlAttrbts.id": FinInstrmGnlAttrbts_id,
                               "FinInstrmGnlAttrbts.FullNm": FinInstrmGnlAttrbts_FullNm,
                               "FinInstrmGnlAttrbts.ClssfctnTp": FinInstrmGnlAttrbts_ClssfctnTp,
                               "FinInstrmGnlAttrbts.CmmdtyDerivInd": FinInstrmGnlAttrbts_CmmdtyDerivInd,
                               "FinInstrmGnlAttrbts.NtnlCcy": FinInstrmGnlAttrbts_NtnlCcy,
                               "Issr": issr}
                    DF = pd.DataFrame(df_data)
                    DF.to_csv("./csvfiles/{}.csv".format(zip_link.rsplit("/", 1)[1].rsplit(".", 1)[0]), index=False)
                else:
                    newfile = "./zipfiles/{}".format(zip_link.rsplit("/", 1)[1].rsplit(".", 1)[0] + ".xml")
                    lxml = etree.parse(newfile)
                    lxml = etree.tostring(lxml, encoding="unicode", pretty_print=True)
                    savexml = open(newfile, "w")
                    savexml.write(lxml)
                    savexml.close()
                    with open(newfile, "r") as file:
                        content = "".join(file.readlines())
                        bs_content2 = bs(content, "xml")
                    for x in bs_content2.findAll("FinInstrmGnlAttrbts"):
                        FinInstrmGnlAttrbts_id.append(x.find("Id").text)
                        FinInstrmGnlAttrbts_FullNm.append(x.find("FullNm").text)
                        FinInstrmGnlAttrbts_ClssfctnTp.append(x.find("ClssfctnTp").text)
                        FinInstrmGnlAttrbts_CmmdtyDerivInd.append(x.find("CmmdtyDerivInd").text)
                        FinInstrmGnlAttrbts_NtnlCcy.append(x.find("NtnlCcy").text)

                    Issr = bs_content2.findAll("Issr")
                    for t in Issr:
                        issr.append(t.text)

                df_data = {"FinInstrmGnlAttrbts.id": FinInstrmGnlAttrbts_id,
                           "FinInstrmGnlAttrbts.FullNm": FinInstrmGnlAttrbts_FullNm,
                           "FinInstrmGnlAttrbts.ClssfctnTp": FinInstrmGnlAttrbts_ClssfctnTp,
                           "FinInstrmGnlAttrbts.CmmdtyDerivInd": FinInstrmGnlAttrbts_CmmdtyDerivInd,
                           "FinInstrmGnlAttrbts.NtnlCcy": FinInstrmGnlAttrbts_NtnlCcy,
                           "Issr": issr}
                DF = pd.DataFrame(df_data)
                DF.to_csv("./csvfiles/{}.csv".format(zip_link.rsplit("/", 1)[1].rsplit(".", 1)[0]), index=False)

        # upload to S3
        res = upload_to_s3("./csvfiles")
        return res
    except Exception as e:
        logger.error("Error in parser_xml : " + "\n" + str(repr(e)) + " line number " + str(sys.exc_info()[2].tb_lineno))


# Read the XML file
with open("main.xml", "r") as file:
    content = "".join(file.readlines())
    bs_content = bs(content, "lxml")

path = os.path.abspath("")
if not os.path.exists("./zipfiles"):
    os.makedirs("./zipfiles")

if not os.path.exists("./csvfiles"):
    os.makedirs("./csvfiles")

res = parser_xml(bs_content)

