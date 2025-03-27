{ pkgs }: {
  deps = [
    pkgs.sqlite
    pkgs.libyaml
    pkgs.glibcLocales
    pkgs.python313Full
  ];
}