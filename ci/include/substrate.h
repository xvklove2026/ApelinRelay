// Minimal substrate declarations for the GitHub Actions build.
//
// On the device Apelin links the real libsubstrate from its support component;
// here only Sources/Tweak.m's single MSHookMessageEx call has to resolve, and
// it is bound at load time by the injector's libsubstrate (-undefined
// dynamic_lookup), exactly as old rootful tweaks do.
#ifndef APELIN_CI_SUBSTRATE_H
#define APELIN_CI_SUBSTRATE_H

#import <objc/runtime.h>

#ifdef __cplusplus
extern "C" {
#endif

void MSHookMessageEx(Class _class, SEL sel, IMP imp, IMP *result);

#ifdef __cplusplus
}
#endif

#endif
