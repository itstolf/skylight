export function throwForStatus(resp: Response): Response {
    if (!resp.ok) {
        throw resp;
    }
    return resp;
}
