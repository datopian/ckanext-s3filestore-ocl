import React from "react";
import ReactDOM from "react-dom/client";
import "./index.css";
import { ResourceConfig } from "./interfaces/common";
import { UploadResult } from "@uppy/core";

const ResourceEl = document.getElementById("ResourceUploader") as HTMLElement;

if (ResourceEl) {
  import("./components/ResourceUI").then((mod) => {
    const ResourceUI = mod.default;
    const ResourceUIRoot = ReactDOM.createRoot(ResourceEl as HTMLElement);
    let config: ResourceConfig = {
      datasetId: ResourceEl.getAttribute("data-dataset-id") || "",
      api: ResourceEl.getAttribute("data-api") || "",
      authToken: ResourceEl.getAttribute("data-auth-token") || "",
      organizationId: ResourceEl.getAttribute("data-organization-id") || "",
      resourceId: ResourceEl.getAttribute("data-resource-id") || "",
      datasetName: ResourceEl.getAttribute("data-dataset-name") || "",
      chunkSize: parseInt(ResourceEl.getAttribute("data-chunk-size") || ""),
      stage: ResourceEl.getAttribute("data-stage") || "",
      csrf: ResourceEl.getAttribute("data-csrf") || "",
    };

    const handleUploadDone: any = (response: UploadResult) => {
      let url = response.successful[0]?.response?.uploadURL as string;
      console.log("RESPONSE", response.successful[0]);
      const urlParts = url.split("/");
      const resourceId = urlParts[urlParts.length - 2];
      const fileName = urlParts[urlParts.length - 1];
      url = `${config.api}/dataset/${config.datasetId}/resource/${resourceId}/${fileName ?? ""}`;
    };

    ResourceUIRoot.render(
      <React.StrictMode>
        <ResourceUI config={config} onUploadSuccess={handleUploadDone} />
      </React.StrictMode>
    );
  });
}
