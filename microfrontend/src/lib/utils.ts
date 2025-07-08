import {
  DatasetSchema,
  DatasetSchemaField,
  DatasetSchemaSelectGroupField,
} from "@/interfaces/dataset";
import { type ClassValue, clsx } from "clsx";
import { format } from "date-fns";
import { twMerge } from "tailwind-merge";
import Papa from "papaparse";

export type HttpMethod = "GET" | "POST";
export type FetchBody = Record<string, any> | null;
export type Token = string | null;

interface FetchOptions {
  api: string;
  action: string;
  headers?: Record<string, string>;
  method: HttpMethod;
  data: FetchBody;
  token: Token;
  csrf?: string;
}

interface CKANRequestOptions {
  action?: string;
  api: string;
  id?: string;
  data?: FetchBody | any;
  token: string;
  csrf?: string;
}

export const fetchAPI = ({
  api,
  action,
  method = "GET",
  headers = {},
  data = null,
  token,
  csrf,
}: FetchOptions) => {
  let url = api + action;

  const options: RequestInit = {
    headers: {
      "Content-Type": "application/json",
    },
    method,
  };

  if (token) {
    options.headers = {
      Authorization: token,
      ...options.headers,
      ...headers,
    };
  }

  if (csrf) {
    options.headers = {
      "X-CSRFToken": csrf,
      ...options.headers,
      ...headers,
    };
  }

  if (method === "POST" && data) {
    options.body = JSON.stringify(data);
  } else if (method === "GET" && data) {
    const queryString = Object.keys(data)
      .map((key) => key + "=" + data[key])
      .join("&");
    url += "?" + queryString;
  }

  return fetch(url, options).then((response) => {
    return response.json();
  });
};

export const getPackage = ({ api, id, token }: CKANRequestOptions) => {
  return fetchAPI({
    api,
    action: "/api/3/action/package_show",
    method: "GET",
    data: { id },
    token,
  });
};

export const createPackage = ({
  api,
  data,
  token,
  csrf,
}: CKANRequestOptions) => {
  return fetchAPI({
    api,
    action: "/api/3/action/package_create",
    method: "POST",
    data,
    token,
    csrf,
  });
};

export const updatePackage = ({
  api,
  data,
  token,
  csrf,
}: CKANRequestOptions) => {
  return fetchAPI({
    api,
    // Validation doesn't seem to be working well with package_patch
    action: "/api/3/action/package_update",
    method: "POST",
    data,
    token,
    csrf,
  });
};

export const patchPackage = ({
  api,
  data,
  token,
  csrf,
}: CKANRequestOptions) => {
  return fetchAPI({
    api,
    // Validation doesn't seem to be working well with package_patch
    action: "/api/3/action/package_patch",
    method: "POST",
    data,
    token,
    csrf,
  });
};

export const deletePackage = ({
  api,
  data,
  token,
  csrf,
}: CKANRequestOptions) => {
  return fetchAPI({
    api,
    action: "/api/3/action/package_delete",
    method: "POST",
    data,
    token,
    csrf,
  });
};

export const getDatasetSchemaList = ({ api, token }: CKANRequestOptions) => {
  return fetchAPI({
    api,
    action: "/api/3/action/scheming_dataset_schema_list",
    method: "GET",
    data: null,
    token,
  });
};

export const getDatasetSchema = ({ api, id, token }: CKANRequestOptions) => {
  const action = "/api/3/action/scheming_dataset_schema_show";
  return fetchAPI({
    api,
    action,
    method: "GET",
    data: { type: id },
    token,
  }).then((res) => res.result);
};

export const authorize = ({ api, id, token, csrf }: CKANRequestOptions) => {
  const action = "/api/3/action/authz_authorize";
  const data = {
    scopes: `ds:${id}:data:*`,
    lifetime: 86400,
  };
  return fetchAPI({
    api,
    action,
    method: "POST",
    data,
    token,
    csrf,
  });
};

export const s3CredentialsCreate = ({
  api,
  data,
  token,
  csrf,
}: CKANRequestOptions) => {
  const action = "/api/3/action/s3_credentials_create";
  return fetchAPI({
    api,
    action,
    method: "POST",
    data: data,
    token,
    csrf,
  });
};

export const s3CredentialsShow = ({
  api,
  data,
  token,
  csrf,
}: CKANRequestOptions) => {
  const action = "/api/3/action/s3_credentials_show";
  return fetchAPI({
    api,
    action,
    method: "GET",
    data: data,
    token,
  });
};

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function packageShowToForm(
  dataset: any,
  schema: DatasetSchema,
  vocabularies: any,
) {
  const defaultValues = structuredClone(dataset);
  defaultValues["state"] = "draft";

  if (defaultValues) {
    if (defaultValues?.tags.length) {
      defaultValues.tag_string = defaultValues.tags.map(
        (t: { name: string; vocabulary_id: string }) => ({
          value: t.name,
          label: t.name,
          vocabulary_id: t.vocabulary_id,
        }),
      );
    }

    const schemaFields = schema.dataset_fields;
    for (let schemaField of schemaFields) {
      const fieldName = schemaField.field_name;
      const currentValue = defaultValues[schemaField.field_name];

      if (schemaField.react_input == "checkbox") {
        if (currentValue != undefined && typeof currentValue == "string") {
          defaultValues[fieldName] = currentValue == "true";
        }
      }

      if (schemaField.react_input === "json") {
        if (currentValue && typeof currentValue != "string") {
          defaultValues[fieldName] = JSON.stringify(currentValue, null, 4);
        }
      }

      if (schemaField.react_input === "date_range") {
        let transformedValue = null;
        if (currentValue) {
          transformedValue = {
            from: currentValue?.start,
            to: currentValue?.end,
          };
        }
        defaultValues[fieldName] = transformedValue;
      }

      if (schemaField.react_input === "select_group") {
        const castedField = schemaField as DatasetSchemaSelectGroupField;
        if ("groups" in defaultValues) {
          const groups = defaultValues["groups"];
          const selectedGroups = groups.filter(
            (g: any) => g.type == castedField.react_input_group_type,
          );

          if (selectedGroups) {
            let newValue;
            if (castedField.react_input_max_selected == 1) {
              newValue = selectedGroups?.at(0)?.name;
            } else {
              newValue = selectedGroups.map((g: any) => ({
                label: g.title,
                value: g.name,
              }));
            }

            defaultValues[fieldName] = newValue;
          }
        }
      }

      if (schemaField.react_input == "date_time_range") {
        const start = defaultValues[fieldName]?.start;
        const end = defaultValues[fieldName]?.end;

        if (!!start && typeof start == "string") {
          const startDate = new Date(start);
          defaultValues[fieldName]["start"] = startDate;
        }

        if (!!end && typeof end == "string") {
          const endDate = new Date(end);
          defaultValues[fieldName]["end"] = endDate;
        }
      }

      if (schemaField.react_input === "multi_select") {
        if (!Array.isArray(currentValue) && currentValue) {
          let segments = currentValue.split(",");

          if (segments.length === 1) {
            defaultValues[fieldName] = [
              { label: currentValue, value: currentValue },
            ];
          } else {
            const itemsAr = JSON.parse(currentValue);
            defaultValues[fieldName] = itemsAr.map((item: string) => ({
              label: item,
              value: item,
            }));
          }
        }
      }

      if (schemaField.react_input == "spatial") {
        if (currentValue) {
          const spatialType = currentValue.spatial_type;

          if (spatialType) {
            const newValue: any = {};
            newValue.spatial_type = spatialType;
            const valueKey = `${spatialType}_value`;
            if ("value" in currentValue) {
              newValue[valueKey] = currentValue.value;
            }
            defaultValues[fieldName] = newValue;
          }
        }
      }

      if (schemaField.react_input == "vocabulary") {
        const namePrefix = schemaField.field_name;
        const associatedVocabs = vocabularies.filter((v: any) =>
          v.name.startsWith(`${namePrefix}_`),
        );

        for (let associatedVocab of associatedVocabs) {
          const associatedTags = defaultValues[namePrefix].filter(
            (t: any) => t.vocabulary_id == associatedVocab.id,
          );

          defaultValues[associatedVocab.name] = associatedTags.map(
            (t: any) => ({
              label: t.name,
              value: t.name,
              vocabulary_id: t.vocabulary_id,
            }),
          );
        }
      }
    }
  }

  return defaultValues;
}

export function formToPackage(data: any, schema: DatasetSchema) {
  const packageData = structuredClone(data);

  const schemaFields = schema.dataset_fields;
  for (let schemaField of schemaFields) {
    const fieldName = schemaField.field_name;
    const reactInput = schemaField.react_input;

    if (reactInput === "multi_select" || reactInput === "tags") {
      if (fieldName in packageData && packageData[fieldName]) {
        packageData[fieldName] = packageData[fieldName].map(
          (f: any) => f.value,
        );
      }
    }

    if (reactInput === "tags") {
      if (fieldName in packageData && packageData[fieldName]) {
        delete packageData["tags"];
        packageData["tags"] = packageData["tag_string"].map((t: string) => ({
          name: t,
        }));
      }
    }

    if (reactInput === "multi_select") {
      if (fieldName in packageData && packageData[fieldName]) {
        if (packageData[fieldName]?.length === 0) {
          delete packageData[fieldName];
        } else if (packageData[fieldName]?.length === 1) {
          packageData[fieldName] = packageData[fieldName].at(0);
        }
      }
    }

    if (reactInput == "spatial") {
      if (fieldName in packageData && packageData[fieldName]) {
        let spatialType = null;
        let newData: any = {};
        if ("spatial_type" in packageData[fieldName]) {
          spatialType = packageData[fieldName]?.spatial_type;
          newData.spatial_type = spatialType;
        }

        let spatialValue = null;
        let valueKey = `${spatialType}_value`;
        if (valueKey in packageData[fieldName]) {
          spatialValue = packageData[fieldName][valueKey];
          newData.value = spatialValue;
        }

        if (spatialType && spatialValue) {
          packageData[fieldName] = newData;

          if (
            spatialType == "bbox" &&
            !!spatialValue &&
            Array.isArray(spatialValue)
          ) {
            let i;
            for (i = 0; i < spatialValue.length; i++) {
              try {
                packageData[fieldName].value[i] = parseFloat(
                  packageData[fieldName].value[i],
                );
              } catch (e) {
                console.error(e);
              }
            }
          }
        } else {
          delete packageData[fieldName];
        }
      }
    }

    if (reactInput === "date_range") {
      if (fieldName in packageData && packageData[fieldName]) {
        if (!packageData[fieldName]) {
          delete packageData[fieldName];
        } else {
          const fieldValue = packageData[fieldName];
          let from = fieldValue?.from;
          let to = fieldValue?.to;
          let startEndFormat: any = {};

          if (from) {
            from = new Date(from);
            from = new Date(
              from.valueOf() + from.getTimezoneOffset() * 60 * 1000,
            );
            from = format(from, "yyyy-MM-dd");
            startEndFormat["start"] = from;
          }

          if (to) {
            to = new Date(to);
            to = new Date(to.valueOf() + to.getTimezoneOffset() * 60 * 1000);
            to = format(to, "yyyy-MM-dd");
            startEndFormat["end"] = to;
          }

          packageData[fieldName] = startEndFormat;
        }
      }
    }

    if (reactInput === "select_group") {
      const castedField = schemaField as DatasetSchemaSelectGroupField;
      let currentValue = packageData[fieldName];

      if (currentValue && typeof currentValue == "string") {
        // This is necessary for single selects e.g. institution
        currentValue = [{ value: currentValue }];
      }

      if (
        currentValue &&
        Array.isArray(currentValue) &&
        currentValue.length > 0
      ) {
        if (!("groups" in packageData)) {
          packageData["groups"] = [];
        }

        // Remove all values of this group type
        packageData["groups"] = packageData["groups"].filter(
          (g: any) => g.type != castedField.react_input_group_type,
        );

        // Add new values to the groups value
        for (let group of currentValue) {
          packageData["groups"].push({
            name: group.value,
            type: castedField.react_input_group_type,
          });
        }

        packageData[fieldName] = currentValue.map((g: any) => g.value);
      } else {
        delete packageData[fieldName];
      }
    }
  }

  // Won't use the iteration above to ensure
  // vocabularies are always processed last
  const vocabularyFields = schemaFields.filter(
    (f) => f.react_input == "vocabulary",
  );

  for (let vocabularyField of vocabularyFields) {
    const fieldName = vocabularyField.field_name;

    // Find data fields that start with "{fieldName}_"
    // e.g. "theme_gmat"
    const vocabularySubFieldsKeys = Object.keys(packageData).filter((f) =>
      f.startsWith(`${fieldName}_`),
    );

    const tagsToAppend = [];
    for (let vocabularySubFieldKey of vocabularySubFieldsKeys) {
      const tags = (packageData[vocabularySubFieldKey] ?? []).map((t: any) => ({
        name: t.value,
        vocabulary_id: t.vocabulary_id,
      }));
      tagsToAppend.push(...tags);
      delete packageData[vocabularySubFieldKey];
    }

    packageData[fieldName] = tagsToAppend.length
      ? JSON.stringify(tagsToAppend)
      : undefined;
  }

  packageData["type"] = schema.dataset_type;

  if (packageData?.access_rights !== "embargoed") {
    packageData["embargoed_until"] = null;
  }

  return packageData;
}

export async function getVocabularies({
  api: apiUrl,
  token,
}: {
  api: string;
  token: string;
}) {
  const response = await fetch(`${apiUrl}/api/3/action/vocabulary_list`);
  const responseBody = await response.json();
  return responseBody.result;
}

export async function tagAutocomplete({
  value,
  vocabularyId,
  apiUrl,
}: {
  value: string;
  vocabularyId?: string;
  apiUrl: string;
}) {
  const res = await fetch(
    `${apiUrl}/api/2/util/tag/autocomplete?incomplete=${
      value ?? ""
    }&vocabulary_id=${vocabularyId}`,
  );
  const jsonBody = await res.json();

  return jsonBody.ResultSet.Result;
}

export const getDeeplyNestedErrors = (
  node: any,
  path: string[] = [],
  errors: any = {},
) => {
  if (typeof node != "string") {
    for (let k of Object.keys(node)) {
      let newPath: string[];
      const newNode = node[k];

      if (path) {
        if (typeof newNode != "string") {
          newPath = path.concat(k);
        } else {
          newPath = path;
        }
      } else {
        newPath = [k];
      }

      getDeeplyNestedErrors(node[k], newPath, errors);
    }
  } else {
    // NOTE: CKAN restricts field error objects to arrays, making it very
    // difficulty to handle fields such as spatial value for bbox.

    const newPath = path.filter((v, i, a) => {
      const nextI = i + 1;
      if (a.length > nextI && a[nextI].includes(".")) {
        return false;
      }
      return true;
    });

    // Special case for wkt_value
    let stringifiedPath = newPath.join(".");
    if (stringifiedPath.endsWith(".")) {
      stringifiedPath = stringifiedPath.slice(0, -1);
    }

    if (stringifiedPath) {
      if (stringifiedPath in errors) {
        errors[stringifiedPath].push(node);
      } else {
        errors[stringifiedPath] = [node];
      }
    }
  }

  return errors;
};

export const getFieldGroupsFromSchema = (
  datasetFieldGroups: DatasetSchema["dataset_fields_groups"],
  datasetFields: DatasetSchemaField[],
) => {
  const fieldGroups = [
    ...datasetFieldGroups,
    { name: "others", label: "Others" },
  ].filter((fg) =>
    datasetFields.some(
      (df) =>
        df.group_name == fg.name || (!df.group_name && fg.name == "others"),
    ),
  );

  return fieldGroups;
};

export const getLicenses = async ({
  api,
  token,
}: {
  api: string;
  token: string;
}) => {
  const response = await fetch(`${api}/api/3/action/license_list`, {
    headers: { Authorization: token },
  });
  const respondeJson = await response.json();
  return respondeJson.result;
};

export const getRemoteCsvChunk = async (
  url: string,
  offset: number,
  bytes: number,
  token: string,
) => {
  const response = await fetch(url, {
    method: "GET",
    headers: {
      Range: `bytes=${offset * bytes}-${offset * bytes + bytes - 1}`,
      Authorization: token,
    },
  });
  if (!response.ok) {
    throw response;
  }
  return response;
};

export function parseContentRange(input: string): any | null {
  const matches = input.match(/^(\w+) ((\d+)-(\d+)|\*)\/(\d+|\*)$/);
  if (!matches) return null;
  const [, unit, , start, end, size] = matches;
  const range = {
    unit,
    start: start != null ? Number(start) : null,
    end: end != null ? Number(end) : null,
    size: size === "*" ? null : Number(size),
  };
  if (range.start === null && range.end === null && range.size === null)
    return null;
  return range;
}

export const getRemoteCsvLines = async (
  url: string,
  lines: number,
  chunkBytesSize: number,
  token: string,
) => {
  let csvString = "";
  let offset = 0;
  let fileSize = -1;
  let linesCount = -1;

  do {
    const chunkRes = await getRemoteCsvChunk(
      url,
      offset,
      chunkBytesSize,
      token,
    );
    const chunk = await chunkRes.text();
    const contentRange = chunkRes.headers.get("Content-Range");
    if (contentRange != null) {
      const range = parseContentRange(contentRange as string);
      fileSize = range.size;
    }

    csvString += chunk;
    offset++;
    linesCount = csvString.split("\n").length;
    // NOTE: extra lines are fetched to ensure last
    // line is completely fetched
  } while (
    offset * chunkBytesSize < fileSize &&
    linesCount <= lines + 3 &&
    // NOTE: if filesize is -1, content-range is not available
    fileSize != -1
  );

  try {
    let csvStringLines = csvString.split("\n");
    csvStringLines = csvStringLines.filter(
      (l) => !l.split("").every((c, i, a) => c == a[0]),
    );
    csvString = csvStringLines.join("\n");
  } catch (e) {
    console.error(e);
  }

  const parsedCsv = await parseCsv(csvString);

  // Checking whether the entire file was loaded
  // or not
  const isEntireFile = linesCount <= lines || fileSize <= chunkBytesSize;
  return {
    data: parsedCsv.data.slice(0, isEntireFile ? undefined : lines),
    isEntireFile,
  };
};

export async function parseCsv(
  csvString: string,
  parsingConfig: any = {},
): Promise<any> {
  return new Promise((resolve, reject) => {
    Papa.parse(csvString, {
      ...parsingConfig,
      header: true,
      dynamicTyping: true,
      skipEmptyLines: true,
      transform: (value: string): string => {
        return value.trim();
      },
      complete: (results: any) => {
        return resolve(results);
      },
      error: (error: any) => {
        console.error(error);
        return reject(error);
      },
    });
  });
}

export const getGroups = async ({
  type,
  api,
  token,
}: {
  type: "institution" | "subject";
  api: string;
  token: string;
}) => {
  const response = await fetch(
    `${api}/api/3/action/group_list?type=${type}&all_fields=True&sort=title asc&include_empty_groups=True&include_extras=True`,
    {
      headers: { Authorization: token },
    },
  );
  const respondeJson = await response.json();
  return respondeJson.result;
};
