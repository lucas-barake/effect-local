export const Platform = {
  OS: "ios",
  select: <A,>(options: { readonly ios?: A; readonly native?: A; readonly default?: A }) =>
    options.ios ?? options.native ?? options.default
}
