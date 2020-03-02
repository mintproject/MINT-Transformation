import { Location } from "history";
import { matchPath } from "react-router";

class Path<URLArgs, QueryArgs> {
  // definition of a path in react-router styles. e.g., /accounts/:id
  public pathDef: string;
  // is equivalent to the `exact` property of the Route component in react-router (whether it should match with its descendant)
  public exact: boolean;
  // equivalent to `strict`
  public strict: boolean;
  // hold properties of Route component in react-router
  public routeDef: { path: string; exact: boolean; strict: boolean };

  public constructor(
    pathDef: string,
    exact: boolean = false,
    strict: boolean = false
  ) {
    this.pathDef = pathDef;
    this.exact = exact;
    this.strict = strict;
    this.routeDef = { path: pathDef, exact, strict };
  }

  /**
   * Create a path based on the given arguments
   */
  public build(urlArgs: URLArgs, queryArgs: QueryArgs): string {
    let path = this.pathDef;

    if (urlArgs !== null) {
      for (let v in urlArgs) {
        path = path.replace(`:${v}`, (urlArgs[v] as any) as string);
      }
    }

    if (queryArgs !== null) {
      path = `${path}?${new URLSearchParams(queryArgs as any).toString()}`;
    }

    return path;
  }

  /**
   * Create a location that the history object can be pushed
   */
  public location(urlArgs: URLArgs, queryArgs: QueryArgs): Location<any> {
    let path = this.pathDef;
    if (urlArgs !== null) {
      for (let v in urlArgs) {
        path = path.replace(`:${v}`, (urlArgs[v] as any) as string);
      }
    }

    return {
      pathname: path,
      search: queryArgs as any,
      hash: "",
      state: undefined
    };
  }
}

/**
 * Find the path that matches with the current location
 */
export function getActivatePath(location: Location<any>) {
  for (let route of Object.values(RouteConf)) {
    if (matchPath(location.pathname, route.routeDef) !== null) {
      return route;
    }
  }
}

export type RouteURLArgs_entity = { entityId: string };

export const RouteConf = {
  home: new Path<null, null>("/", true)
};
