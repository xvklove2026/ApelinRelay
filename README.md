# Apelin Relay

A tweak that runs an in-process translation service inside SpringBoard: it
listens on `127.0.0.1:18791` and speaks both the OpenAI `chat/completions` API
and the Anthropic `messages` API, forwarding to xingchiapi's `/v1/responses`
endpoint and re-framing the streamed answer for the caller.

It is also injected into Apelin itself (`wiki.qaq.apelin`), where it shows a
small translucent pill with the turn count and a long-press sheet that sets the
forwarded history window.

## Why this repository builds it

The target device is an iPhone X (iPhone10,3, A11) on iOS 16.7.16 with a
roothide bootstrap. A11 supports only `arm64`, and it cannot host any Apple
command line tools (every release needs `FEAT_LRCPC`, which A11 lacks), so the
package is compiled on GitHub's macOS runners instead.

`.github/workflows/build.yml` builds one dylib with **both** slices:

- `arm64e` for A12 and later,
- `arm64` for A11 and earlier — a dylib with only `arm64e` is refused by dyld
  on an A11 device, which is why the relay never started there.

It then assembles a single package with the roothide layout
(`/Library/MobileSubstrate/DynamicLibraries/`) and attaches the `.deb` to a
release.

## Installing

Download `ApelinRelay_*_iphoneos-arm64.deb` from the latest release and install
it with Sileo or Filza, then respring.

## Building locally

`apelin-project.json` describes the same product for Apelin's own build on a
device with a usable toolchain; `ci/` holds the control file, the injection
filter and the minimal `substrate.h` the workflow uses.

## Layout

| Path | What it is |
| --- | --- |
| `Sources/Tweak.m` | the whole tweak: socket listener, both API translations, overlay |
| `apelin-project.json` | Apelin's build description (product, package, architectures) |
| `ci/control`, `ci/ApelinRelay.plist` | package control and tweak filter |
| `.github/workflows/build.yml` | the GitHub Actions build |

## Licence

MIT.
