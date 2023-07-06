# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import traceback
import ppx
import pandas as pd
import time

start = time.time()

project_list = ppx.pride.list_projects()
project_list_2 = []
idx_2 = 0

project_pd = pd.DataFrame(columns=["project", "raws", "submissionDate", "title", "labPIs", "instruments", "organisms", "modifications", "references"])

while len(project_list) > 0 and idx_2 < 20:
    idx_2 += 1
    idx = 0
    print("Round " + str(idx_2))
    for project in project_list:
        idx += 1
        print("Fetching %d out of %d..." % (idx, len(project_list)))
        t = ppx.pride.PrideProject(project)
        try:
            raws = t.remote_files("*.raw")
            title = ""
            labPIs = ""
            instruments = ""
            organisms = ""
            modifications = ""
            references = ""
            if "title" in t.metadata:
                title = t.metadata["title"]
            if "labPIs" in t.metadata:
                labPIs = t.metadata["labPIs"]
            if "instruments" in t.metadata:
                ttt = set()
                for tt in t.metadata["instruments"]:
                    if "name" in tt:
                        ttt.add(tt["name"])
                instruments = ";".join(sorted(ttt))
            if "organisms" in t.metadata:
                ttt = set()
                for tt in t.metadata["organisms"]:
                    if "name" in tt:
                        ttt.add(tt["name"])
                organisms = ";".join(sorted(ttt))
            if "identifiedPTMStrings" in t.metadata:
                ttt = set()
                for tt in t.metadata["identifiedPTMStrings"]:
                    if "name" in tt:
                        ttt.add(tt["name"])
                modifications = ";".join(sorted(ttt))
            if "references" in t.metadata:
                references = str(t.metadata["references"])
            project_pd = pd.concat([project_pd,
                                    pd.DataFrame([{
                                        "project": project,
                                        "raws": len(raws),
                                        "submissionDate": t.metadata["submissionDate"],
                                        "title": title,
                                        "labPIs": labPIs,
                                        "instruments": instruments,
                                        "organisms": organisms,
                                        "modifications": modifications,
                                        "references": references
                                    }])],
                                   ignore_index=True)
        except:
            print("%s: error" % project)
            print(traceback.format_exc())
            project_list_2.append(project)
        time.sleep(5)
    time.sleep(60)

    project_list = project_list_2
    project_list_2 = []

for project in project_list:
    project_pd = pd.concat([project_pd,
                            pd.DataFrame([{
                                "project": project,
                                "raws": 0,
                                "submissionDate": "NA",
                                "title": "NA",
                                "labPIs": "NA",
                                "instruments": "NA",
                                "organisms": "NA",
                                "modifications": "NA",
                                "references": "NA"
                            }])],
                           ignore_index=True)

project_pd.to_csv("pride_projects.tsv", sep="\t", index=False)

print("Done in %.1f s" % (time.time() - start))
