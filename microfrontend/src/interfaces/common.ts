export interface ResourceConfig {
    datasetId: string;
    api: string;
    authToken: string;
    organizationId: string;
    resourceId: string;
    datasetName: string;
    chunkSize?: number | null;
    stage: string;
    csrf: string;
    isVersionOf?: string | null;
    datasetTitle?: string | null;
    tocDownloadURL?: string;
    isDraft?: boolean;
}
