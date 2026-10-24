# Idle frontend rendering

The authenticated shell used to run an infinite `pulse` opacity animation on
`.status-dot` in the common top bar. It ran on every view, even with no requests
or user interaction. An opacity animation can keep the compositor active without
repeated JavaScript, layout or paint work (see [Chrome's rendering explanation](https://web.dev/articles/animations-overview)).
The top bar also used `backdrop-filter: blur(12px)`.

The status dot is now static, and the header uses an opaque background matching
its previous colour over the app background. Persistent update badges and
critical health dots in Stacks are also static. Their colours, text and tooltips
are preserved. Spinners for ongoing work remain animated.

## Verification

- `node --test tests/*.cjs`: 45 tests pass.
- A local headless Chromium check loaded the actual HTML and stylesheets with
  application scripts disabled to isolate rendering from API/production state.
  Revealing the authenticated shell reproduced the infinite `pulse` animation.
  Disabling that animation removed it from `document.getAnimations()` and the
  subsequent three-second trace contained no compositor frame submissions.
- After the fix, the isolated shell has no infinite animations across all seven
  views at regular and mobile (390px) viewport widths, including injected Stacks
  update and critical-health indicators. The header's computed backdrop filter
  is `none`.
- This isolates a continuous rendering source; it does **not** measure hardware
  GPU utilisation or establish that every machine will fall from 20% to a
  particular percentage. Verify on the affected Chrome/device after deployment.

## Check on the affected device

1. Open PulsarCD in Chrome, sign in, and wait for requests and chart entrance
   animations to finish. Ensure no build, test, deploy or log stream is active.
2. Record a Performance trace while idle on Dashboard, Logs, Computers, Usage,
   Security and Stacks. Check Stacks with an update available and a critical
   health indicator too. Use Chrome's task manager or the OS GPU monitor to
   compare the same window size, display refresh rate and hardware before/after.
3. Inspect ongoing animations in DevTools:

   ```js
   document.getAnimations().filter(animation =>
       animation.effect.getTiming().iterations === Infinity
   ).map(animation => ({
       name: animation.animationName,
       target: animation.effect.target
   }));
   ```

   The common header and persistent Stacks indicators should never appear.
   Loading spinners may appear while work is in progress; the terminal's cursor
   and actively streaming data are separate sources of intentional rendering.
4. Verify that update/critical indicators remain visible and that loading
   spinners and navigation still work. Repeat with a mobile-sized viewport.
