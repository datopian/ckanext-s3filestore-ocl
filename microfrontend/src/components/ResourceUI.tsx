import React, { useEffect, useState } from "react";
import Uppy, { UppyFile, type UploadResult } from "@uppy/core";
import { Dashboard } from "@uppy/react";
// @ts-ignore
import AwsS3Multipart from "@uppy/aws-s3-multipart";
import "@uppy/core/dist/style.min.css";
import "@uppy/dashboard/dist/style.min.css";
import { ResourceConfig } from "../interfaces/common";

const fetchUploadApiEndpoint = async (
  apiToken: string,
  endpoint: string,
  data: any
) => {
  const res = await fetch(
    `${window.location.origin}/api/3/action/${endpoint}`,
    {
      method: "POST",
      credentials: "omit",
      body: JSON.stringify(data),
      headers: {
        accept: "application/json",
        "Content-Type": "application/json",
        Authorization: apiToken,
      },
    }
  );

  return (await res.json()).result;
};

export async function getUploadParameters(
  apiToken: string,
  package_id: string,
  file: UppyFile
) {
  const response = await fetch(
    `${window.location.origin}/api/3/action/get_signed_url`,
    {
      method: "POST",
      credentials: "omit",
      headers: {
        accept: "application/json",
        "Content-Type": "application/json",
        Authorization: apiToken,
      },
      body: JSON.stringify({
        package_id: package_id,
        filename: file.name,
        contentType: file.type,
      }),
    }
  );
  if (!response.ok) throw new Error("Unsuccessful request");

  const data: { signed_url: string } = (await response.json()).result;

  const object = {
    method: "PUT",
    url: data.signed_url,
    fields: {},

    headers: {
      "Content-Type": file.type ? file.type : "application/octet-stream",
    },
  };
  return object;
}

export default function MultipartFileUploader({
  onUploadSuccess,
  config,
}: {
  config: ResourceConfig;
  onUploadSuccess: (result: UploadResult) => void;
}) {
  const [hideUploader, setHideUploader] = useState(false);
  useEffect(() => {
    document
      .getElementById("resource-link-button")
      ?.addEventListener("click", () => setHideUploader(true));
  }, []);
  const uppy = React.useMemo(() => {
    const uppy = new Uppy({
      restrictions: {
        maxNumberOfFiles: 1,
      },
      autoProceed: true,
    }).use(
      AwsS3Multipart as any,
      {
        getUploadParameters: (file: UppyFile) =>
          getUploadParameters(config.authToken, config.datasetId, file),
        createMultipartUpload: async (file: any) => {
          const contentType = file.type;
          return fetchUploadApiEndpoint(
            config.authToken,
            "create-multipart-upload",
            {
              ...file,
              contentType,
              package_id: config.datasetId,
            }
          );
        },
        listParts: (file: any, props: any) =>
          fetchUploadApiEndpoint(config.authToken, "list-parts", {
            file,
            package_id: config.datasetId,
            ...props,
          }),
        signPart: (file: any, props: any) =>
          fetchUploadApiEndpoint(config.authToken, "sign-part", {
            file,
            package_id: config.datasetId,
            ...props,
          }),
        abortMultipartUpload: (file: any, props: any) =>
          fetchUploadApiEndpoint(config.authToken, "abort-multipart-upload", {
            file,
            package_id: config.datasetId,
            ...props,
          }),
        completeMultipartUpload: (file: any, props: any) =>
          fetchUploadApiEndpoint(
            config.authToken,
            "complete-multipart-upload",
            {
              ...file,
              ...props,
              package_id: config.datasetId,
            }
          ),
      } as any
    );

    uppy.on("file-added", (file) => {
      const fileCount = Object.keys(uppy.getState().files).length;
      if (fileCount > 1) {
        uppy.removeFile(file.id);
        uppy.info(
          "Only one file is allowed. Please remove the existing file first.",
          "error",
          3000
        );
      }
    });

    uppy.on("file-removed", (file) => {
      (
        (document.getElementById("resource-url-link") as any) ?? {
          checked: false,
        }
      ).checked = true;
      document.getElementById("field-resource-url")?.focus();
      document.getElementById("field-resource-url")?.setAttribute("value", "");
      for (let ckanRemoveButton of (document.querySelectorAll(
        ".btn.btn-danger.btn-remove-url"
      ) as any) ?? []) {
        setTimeout(() => ckanRemoveButton.click(), 15);
      }
    });

    for (let ckanRemoveButton of (document.querySelectorAll(
      ".btn.btn-danger.btn-remove-url"
    ) as any) ?? []) {
      ckanRemoveButton.addEventListener("click", () => {
        uppy.cancelAll();
      });
    }

    return uppy;
  }, []);

  uppy.on("complete", (result) => {
    (
      (document.getElementById("resource-url-upload") as any) ?? {
        checked: false,
      }
    ).checked = true;

    replaceURLInput(result.successful[0].name);
    (
      (document.getElementById("field-name") as HTMLInputElement) ?? {
        value: "",
      }
    ).value = result.successful[0]?.name;

    onUploadSuccess(result);
  });

  uppy.on("upload-success", (file, response) => {
    uppy.setFileState(file!.id, {
      progress: uppy.getState().files[file!.id].progress,
      uploadURL: response.body.Location,
      response: response,
      isPaused: false,
    });
    replaceURLInput(file?.name);
  });

  const replaceURLInput = (filename?: string) => {
    const urlInput = document.getElementById(
      "field-resource-url"
    ) as HTMLInputElement | null;
    urlInput?.setAttribute(
      "value",
      `${window.location.origin}/dataset/${config.datasetId}/resource/${
        config.resourceId ? config.resourceId : "REPLACE_HERE"
      }/${filename ? filename : ""}`
    );
  };

  return (
    <>
      {!hideUploader ? (
        <Dashboard
          uppy={uppy}
          showLinkToFileUploadResult={true}
          hideUploadButton={true}
          hideCancelButton={true}
          hidePauseResumeButton={true}
          hideProgressAfterFinish={true}
          showRemoveButtonAfterComplete={true}
        />
      ) : (
        <></>
      )}
    </>
  );
}
